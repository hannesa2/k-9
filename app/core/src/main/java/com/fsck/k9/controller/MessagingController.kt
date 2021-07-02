package com.fsck.k9.controller

import com.fsck.k9.DI.get
import com.fsck.k9.search.getAccounts
import com.fsck.k9.notification.NotificationController
import com.fsck.k9.notification.NotificationStrategy
import com.fsck.k9.mailstore.LocalStoreProvider
import com.fsck.k9.backend.BackendManager
import com.fsck.k9.mailstore.MessageStoreManager
import com.fsck.k9.mailstore.SaveMessageDataCreator
import com.fsck.k9.controller.ControllerExtension.ControllerInternals
import timber.log.Timber
import com.fsck.k9.mailstore.LocalStore
import com.fsck.k9.mail.MessagingException
import com.fsck.k9.mailstore.LocalMessage
import com.fsck.k9.cache.EmailProviderCache
import com.fsck.k9.search.LocalSearch
import com.fsck.k9.mail.MessageRetrievalListener
import com.fsck.k9.mailstore.LocalFolder
import com.fsck.k9.backend.api.SyncConfig
import com.fsck.k9.helper.ExceptionHelper
import com.fsck.k9.K9
import com.fsck.k9.controller.MessagingControllerCommands.PendingCommand
import com.fsck.k9.mailstore.UnavailableStorageException
import com.fsck.k9.controller.MessagingControllerCommands.PendingAppend
import com.fsck.k9.mail.FetchProfile
import com.fsck.k9.controller.MessagingControllerCommands.PendingReplace
import com.fsck.k9.controller.MessagingControllerCommands.PendingMoveOrCopy
import com.fsck.k9.controller.MessagingControllerCommands.PendingMoveAndMarkAsRead
import com.fsck.k9.Account.Expunge
import com.fsck.k9.controller.MessagingControllerCommands.PendingSetFlag
import com.fsck.k9.controller.MessagingControllerCommands.PendingDelete
import com.fsck.k9.controller.MessagingControllerCommands.PendingExpunge
import com.fsck.k9.controller.MessagingControllerCommands.PendingMarkAllAsRead
import com.fsck.k9.mailstore.SendState
import com.fsck.k9.mail.AuthenticationFailedException
import com.fsck.k9.mail.CertificateValidationException
import com.fsck.k9.search.SearchAccount
import android.annotation.SuppressLint
import android.content.Context
import android.os.PowerManager
import android.os.Process
import android.os.SystemClock
import android.widget.Toast
import androidx.annotation.VisibleForTesting
import com.fsck.k9.Account
import com.fsck.k9.Account.DeletePolicy
import com.fsck.k9.controller.MessagingControllerCommands.PendingEmptyTrash
import com.fsck.k9.power.TracingPowerManager.TracingWakeLock
import com.fsck.k9.power.TracingPowerManager
import com.fsck.k9.backend.api.SyncListener
import com.fsck.k9.Preferences
import com.fsck.k9.backend.api.Backend
import com.fsck.k9.backend.api.BuildConfig
import com.fsck.k9.helper.MutableBoolean
import com.fsck.k9.helper.Preconditions
import com.fsck.k9.mail.Flag
import com.fsck.k9.mail.Message
import com.fsck.k9.mail.Part
import java.lang.AssertionError
import java.lang.Error
import java.lang.Exception
import java.lang.IllegalArgumentException
import java.lang.IllegalStateException
import java.lang.RuntimeException
import java.lang.UnsupportedOperationException
import java.util.ArrayList
import java.util.EnumSet
import java.util.HashMap
import java.util.HashSet
import java.util.LinkedList
import java.util.concurrent.BlockingQueue
import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

/**
 * Starts a long running (application) Thread that will run through commands
 * that require remote mailbox access. This class is used to serialize and
 * prioritize these commands. Each method that will submit a command requires a
 * MessagingListener instance to be provided. It is expected that that listener
 * has also been added as a registered listener using addListener(). When a
 * command is to be executed, if the listener that was provided with the command
 * is no longer registered the command is skipped. The design idea for the above
 * is that when an Activity starts it registers as a listener. When it is paused
 * it removes itself. Thus, any commands that that activity submitted are
 * removed from the queue once the activity is no longer active.
 */
class MessagingController internal constructor(
    private val context: Context, private val notificationController: NotificationController,
    private val notificationStrategy: NotificationStrategy, private val localStoreProvider: LocalStoreProvider,
    private val unreadMessageCountProvider: UnreadMessageCountProvider, private val backendManager: BackendManager,
    private val preferences: Preferences, private val messageStoreManager: MessageStoreManager,
    private val saveMessageDataCreator: SaveMessageDataCreator, controllerExtensions: List<ControllerExtension>
) {
    private val controllerThread: Thread
    private val queuedCommands: BlockingQueue<Command> = PriorityBlockingQueue()
    val messagesListeners: MutableSet<MessagingListener> = CopyOnWriteArraySet()
    private val threadPool = Executors.newCachedThreadPool()
    private val memorizingMessagingListener = MemorizingMessagingListener()
    private val draftOperations: DraftOperations

    @Volatile
    private var stopped = false
    private fun initializeControllerExtensions(controllerExtensions: List<ControllerExtension>) {
        if (controllerExtensions.isEmpty()) {
            return
        }
        val internals: ControllerInternals = object : ControllerInternals {
            override fun put(
                description: String, listener: MessagingListener?,
                runnable: Runnable
            ) {
                this@MessagingController.put(description, listener, runnable)
            }

            override fun putBackground(
                description: String, listener: MessagingListener?,
                runnable: Runnable
            ) {
                this@MessagingController.putBackground(description, listener, runnable)
            }
        }
        for (extension in controllerExtensions) {
            extension.init(this, backendManager, internals)
        }
    }

    @VisibleForTesting
    @Throws(InterruptedException::class)
    fun stop() {
        stopped = true
        controllerThread.interrupt()
        controllerThread.join(1000L)
    }

    private fun runInBackground() {
        Process.setThreadPriority(Process.THREAD_PRIORITY_BACKGROUND)
        while (!stopped) {
            var commandDescription: String? = null
            try {
                val command = queuedCommands.take()
                if (command != null) {
                    commandDescription = command.description
                    Timber.i(
                        "Running command '%s', seq = %s (%s priority)",
                        command.description,
                        command.sequence,
                        if (command.isForegroundPriority) "foreground" else "background"
                    )
                    try {
                        command.runnable!!.run()
                    } catch (e: UnavailableAccountException) {
                        // retry later
                        object : Thread() {
                            override fun run() {
                                try {
                                    sleep((30 * 1000).toLong())
                                    queuedCommands.put(command)
                                } catch (e: InterruptedException) {
                                    Timber.e("Interrupted while putting a pending command for an unavailable account  back into the queue. THIS SHOULD NEVER HAPPEN.")
                                }
                            }
                        }.start()
                    }
                    Timber.i(" Command '%s' completed", command.description)
                }
            } catch (e: Exception) {
                Timber.e(e, "Error running command '%s'", commandDescription)
            }
        }
    }

    private fun put(description: String, listener: MessagingListener?, runnable: Runnable) {
        putCommand(queuedCommands, description, listener, runnable, true)
    }

    private fun putBackground(description: String, listener: MessagingListener?, runnable: Runnable) {
        putCommand(queuedCommands, description, listener, runnable, false)
    }

    private fun putCommand(
        queue: BlockingQueue<Command>, description: String, listener: MessagingListener?,
        runnable: Runnable, isForeground: Boolean
    ) {
        var retries = 10
        var e: Exception? = null
        while (retries-- > 0) {
            try {
                val command = Command()
                command.listener = listener
                command.runnable = runnable
                command.description = description
                command.isForegroundPriority = isForeground
                queue.put(command)
                return
            } catch (ie: InterruptedException) {
                SystemClock.sleep(200)
                e = ie
            }
        }
        throw Error(e)
    }

    fun getBackend(account: Account?): Backend {
        return backendManager.getBackend(account!!)
    }

    fun getLocalStoreOrThrow(account: Account): LocalStore {
        return try {
            localStoreProvider.getInstance(account)
        } catch (e: MessagingException) {
            throw IllegalStateException("Couldn't get LocalStore for account " + account.description)
        }
    }

    @Throws(MessagingException::class)
    private fun getFolderServerId(account: Account, folderId: Long): String {
        val localStore = getLocalStoreOrThrow(account)
        return localStore.getFolderServerId(folderId)
    }

    @Throws(MessagingException::class)
    private fun getFolderId(account: Account, folderServerId: String): Long {
        val localStore = getLocalStoreOrThrow(account)
        return localStore.getFolderId(folderServerId)
    }

    private fun getFolderIdOrThrow(account: Account, folderServerId: String): Long {
        val localStore = getLocalStoreOrThrow(account)
        return try {
            localStore.getFolderId(folderServerId)
        } catch (e: MessagingException) {
            throw IllegalStateException(e)
        }
    }

    fun addListener(listener: MessagingListener) {
        messagesListeners.add(listener)
        refreshListener(listener)
    }

    private fun refreshListener(listener: MessagingListener?) {
        if (listener != null) {
            memorizingMessagingListener.refreshOther(listener)
        }
    }

    fun removeListener(listener: MessagingListener) {
        messagesListeners.remove(listener)
    }

    fun getListeners(listener: MessagingListener?): Set<MessagingListener> {
        val listeners: MutableSet<MessagingListener> = HashSet(messagesListeners)
        listener?.let {
            listeners.add(listener)
        }
        return listeners
    }

    private fun suppressMessages(account: Account, messages: List<LocalMessage>) {
        val cache = EmailProviderCache.getCache(account.uuid, context)
        cache.hideMessages(messages)
    }

    private fun unsuppressMessages(account: Account, messages: List<LocalMessage>) {
        val cache = EmailProviderCache.getCache(account.uuid, context)
        cache.unhideMessages(messages)
    }

    fun isMessageSuppressed(message: LocalMessage): Boolean {
        val messageId = message.databaseId
        val folderId = message.folder.databaseId
        val cache = EmailProviderCache.getCache(message.folder.accountUuid, context)
        return cache.isMessageHidden(messageId, folderId)
    }

    private fun setFlagInCache(
        account: Account, messageIds: List<Long>,
        flag: Flag, newState: Boolean
    ) {
        val cache = EmailProviderCache.getCache(account.uuid, context)
        val columnName = LocalStore.getColumnNameForFlag(flag)
        val value = Integer.toString(if (newState) 1 else 0)
        cache.setValueForMessages(messageIds, columnName, value)
    }

    private fun removeFlagFromCache(
        account: Account, messageIds: List<Long>,
        flag: Flag
    ) {
        val cache = EmailProviderCache.getCache(account.uuid, context)
        val columnName = LocalStore.getColumnNameForFlag(flag)
        cache.removeValueForMessages(messageIds, columnName)
    }

    private fun setFlagForThreadsInCache(
        account: Account, threadRootIds: List<Long>,
        flag: Flag, newState: Boolean
    ) {
        val cache = EmailProviderCache.getCache(account.uuid, context)
        val columnName = LocalStore.getColumnNameForFlag(flag)
        val value = Integer.toString(if (newState) 1 else 0)
        cache.setValueForThreads(threadRootIds, columnName, value)
    }

    private fun removeFlagForThreadsFromCache(
        account: Account, messageIds: List<Long>,
        flag: Flag
    ) {
        val cache = EmailProviderCache.getCache(account.uuid, context)
        val columnName = LocalStore.getColumnNameForFlag(flag)
        cache.removeValueForThreads(messageIds, columnName)
    }

    fun refreshFolderList(account: Account) {
        put("refreshFolderList", null) { refreshFolderListSynchronous(account) }
    }

    fun refreshFolderListSynchronous(account: Account) {
        try {
            val backend = getBackend(account)
            backend.refreshFolderList()
            val now = System.currentTimeMillis()
            Timber.d("Folder list successfully refreshed @ $now")
            account.lastFolderListRefreshTime = now
            preferences.saveAccount(account)
        } catch (e: Exception) {
            Timber.e(e)
        }
    }

    /**
     * Find all messages in any local account which match the query 'query'
     */
    fun searchLocalMessages(search: LocalSearch, listener: MessagingListener?) {
        threadPool.execute { searchLocalMessagesSynchronous(search, listener) }
    }

    @VisibleForTesting
    fun searchLocalMessagesSynchronous(search: LocalSearch, listener: MessagingListener?) {
        val searchAccounts = search.getAccounts(preferences)
        for (account in searchAccounts) {

            // Collecting statistics of the search result
            val retrievalListener: MessageRetrievalListener<LocalMessage> =
                object : MessageRetrievalListener<LocalMessage> {
                    override fun messageStarted(message: String, number: Int, ofTotal: Int) {}
                    override fun messagesFinished(number: Int) {}
                    override fun messageFinished(message: LocalMessage, number: Int, ofTotal: Int) {
                        if (!isMessageSuppressed(message)) {
                            val messages: MutableList<LocalMessage> = ArrayList()
                            messages.add(message)
                            listener?.listLocalMessagesAddMessages(account, null, messages)
                        }
                    }
                }

            // build and do the query in the localstore
            try {
                val localStore = localStoreProvider.getInstance(account)
                localStore.searchForMessages(retrievalListener, search)
            } catch (e: Exception) {
                Timber.e(e)
            }
        }
        listener?.listLocalMessagesFinished()
    }

    fun searchRemoteMessages(
        acctUuid: String?, folderId: Long, query: String?, requiredFlags: Set<Flag>?,
        forbiddenFlags: Set<Flag>?, listener: MessagingListener?
    ): Future<*> {
        Timber.i("searchRemoteMessages (acct = %s, folderId = %d, query = %s)", acctUuid, folderId, query)
        return threadPool.submit {
            searchRemoteMessagesSynchronous(
                acctUuid,
                folderId,
                query,
                requiredFlags,
                forbiddenFlags,
                listener
            )
        }
    }

    @VisibleForTesting
    fun searchRemoteMessagesSynchronous(
        acctUuid: String?, folderId: Long, query: String?, requiredFlags: Set<Flag>?,
        forbiddenFlags: Set<Flag>?, listener: MessagingListener?
    ) {
        val account = preferences.getAccount(acctUuid!!)
        listener?.remoteSearchStarted(folderId)
        var extraResults: List<String?> = ArrayList()
        try {
            val localStore = localStoreProvider.getInstance(account!!)
            val localFolder = localStore.getFolder(folderId)
            if (!localFolder.exists()) {
                throw MessagingException("Folder not found")
            }
            localFolder.open()
            val folderServerId = localFolder.serverId
            val backend = getBackend(account)
            val performFullTextSearch = account.isRemoteSearchFullText
            var messageServerIds: List<String?> = backend.search(
                folderServerId, query, requiredFlags, forbiddenFlags,
                performFullTextSearch
            )
            Timber.i("Remote search got %d results", messageServerIds.size)

            // There's no need to fetch messages already completely downloaded
            messageServerIds = localFolder.extractNewMessages(messageServerIds)
            listener?.remoteSearchServerQueryComplete(
                folderId, messageServerIds.size,
                account.remoteSearchNumResults
            )
            val resultLimit = account.remoteSearchNumResults
            if (resultLimit > 0 && messageServerIds.size > resultLimit) {
                extraResults = messageServerIds.subList(resultLimit, messageServerIds.size)
                messageServerIds = messageServerIds.subList(0, resultLimit)
            }
            loadSearchResultsSynchronous(account, messageServerIds, localFolder)
        } catch (e: Exception) {
            if (Thread.currentThread().isInterrupted) {
                Timber.i(e, "Caught exception on aborted remote search; safe to ignore.")
            } else {
                Timber.e(e, "Could not complete remote search")
                listener?.remoteSearchFailed(null, e.message)
                Timber.e(e)
            }
        } finally {
            listener?.remoteSearchFinished(folderId, 0, account!!.remoteSearchNumResults, extraResults)
        }
    }

    fun loadSearchResults(
        account: Account?, folderId: Long, messageServerIds: List<String?>,
        listener: MessagingListener?
    ) {
        threadPool.execute {
            listener?.enableProgressIndicator(true)
            try {
                val localStore = localStoreProvider.getInstance(account!!)
                val localFolder = localStore.getFolder(folderId)
                if (!localFolder.exists()) {
                    throw MessagingException("Folder not found")
                }
                localFolder.open()
                loadSearchResultsSynchronous(account, messageServerIds, localFolder)
            } catch (e: MessagingException) {
                Timber.e(e, "Exception in loadSearchResults")
            } finally {
                listener?.enableProgressIndicator(false)
            }
        }
    }

    @Throws(MessagingException::class)
    private fun loadSearchResultsSynchronous(
        account: Account?,
        messageServerIds: List<String?>,
        localFolder: LocalFolder
    ) {
        val backend = getBackend(account)
        val folderServerId = localFolder.serverId
        for (messageServerId in messageServerIds) {
            val localMessage = localFolder.getMessage(messageServerId)
            if (localMessage == null) {
                backend.downloadMessageStructure(folderServerId, messageServerId!!)
            }
        }
    }

    fun loadMoreMessages(account: Account, folderId: Long, listener: MessagingListener?) {
        try {
            val localStore = localStoreProvider.getInstance(account)
            val localFolder = localStore.getFolder(folderId)
            if (localFolder.visibleLimit > 0) {
                localFolder.visibleLimit = localFolder.visibleLimit + account.displayCount
            }
            synchronizeMailbox(account, folderId, listener)
        } catch (me: MessagingException) {
            throw RuntimeException("Unable to set visible limit on folder", me)
        }
    }

    /**
     * Start background synchronization of the specified folder.
     */
    fun synchronizeMailbox(account: Account, folderId: Long, listener: MessagingListener?) {
        putBackground(
            "synchronizeMailbox", listener
        ) { synchronizeMailboxSynchronous(account, folderId, listener) }
    }

    /**
     * Start foreground synchronization of the specified folder. This is generally only called
     * by synchronizeMailbox.
     *
     *
     * TODO Break this method up into smaller chunks.
     */
    @VisibleForTesting
    fun synchronizeMailboxSynchronous(account: Account, folderId: Long, listener: MessagingListener?) {
        refreshFolderListIfStale(account)
        val backend = getBackend(account)
        syncFolder(account, folderId, listener, backend)
    }

    private fun refreshFolderListIfStale(account: Account) {
        val lastFolderListRefresh = account.lastFolderListRefreshTime
        val now = System.currentTimeMillis()
        if (lastFolderListRefresh > now || lastFolderListRefresh + FOLDER_LIST_STALENESS_THRESHOLD <= now) {
            Timber.d("Last folder list refresh @ $lastFolderListRefresh. Refreshing now…")
            refreshFolderListSynchronous(account)
        } else {
            Timber.d("Last folder list refresh @ $lastFolderListRefresh. Not refreshing now.")
        }
    }

    private fun syncFolder(account: Account, folderId: Long, listener: MessagingListener?, backend: Backend) {
        var commandException: Exception? = null
        try {
            processPendingCommandsSynchronous(account)
        } catch (e: Exception) {
            Timber.e(e, "Failure processing command, but allow message sync attempt")
            commandException = e
        }
        val localFolder: LocalFolder
        try {
            val localStore = localStoreProvider.getInstance(account)
            localFolder = localStore.getFolder(folderId)
            localFolder.open()
        } catch (e: MessagingException) {
            Timber.e(e, "syncFolder: Couldn't load local folder %d", folderId)
            return
        }

        // We can't sync local folders
        if (localFolder.isLocalOnly) {
            return
        }
        val folderServerId = localFolder.serverId
        val syncConfig = createSyncConfig(account)
        val syncListener = ControllerSyncListener(account, listener)
        backend.sync(folderServerId, syncConfig, syncListener)
        if (commandException != null && !syncListener.syncFailed) {
            val rootMessage = ExceptionHelper.getRootCauseMessage(commandException)
            Timber.e("Root cause failure in %s:%s was '%s'", account.description, folderServerId, rootMessage)
            updateFolderStatus(account, folderServerId, rootMessage)
            listener?.synchronizeMailboxFailed(account, folderId, rootMessage)
        }
    }

    private fun createSyncConfig(account: Account): SyncConfig {
        return SyncConfig(
            account.expungePolicy.toBackendExpungePolicy(),
            account.earliestPollDate,
            account.isSyncRemoteDeletions,
            account.maximumAutoDownloadMessageSize,
            K9.DEFAULT_VISIBLE_LIMIT,
            SYNC_FLAGS
        )
    }

    private fun updateFolderStatus(account: Account, folderServerId: String, status: String) {
        try {
            val localStore = localStoreProvider.getInstance(account)
            val localFolder = localStore.getFolder(folderServerId)
            localFolder.status = status
        } catch (e: MessagingException) {
            Timber.w(e, "Couldn't update folder status for folder %s", folderServerId)
        }
    }

    fun handleAuthenticationFailure(account: Account?, incoming: Boolean) {
        notificationController.showAuthenticationErrorNotification(account, incoming)
    }

    fun queuePendingCommand(account: Account?, command: PendingCommand?) {
        try {
            val localStore = localStoreProvider.getInstance(account!!)
            localStore.addPendingCommand(command)
        } catch (e: Exception) {
            throw RuntimeException("Unable to enqueue pending command", e)
        }
    }

    fun processPendingCommands(account: Account?) {
        putBackground("processPendingCommands", null) {
            try {
                processPendingCommandsSynchronous(account)
            } catch (e: UnavailableStorageException) {
                Timber.i("Failed to process pending command because storage is not available -  trying again later.")
                throw UnavailableAccountException(e)
            } catch (me: MessagingException) {
                Timber.e(me, "processPendingCommands")

                /*
                         * Ignore any exceptions from the commands. Commands will be processed
                         * on the next round.
                         */
            }
        }
    }

    @Throws(MessagingException::class)
    fun processPendingCommandsSynchronous(account: Account?) {
        val localStore = localStoreProvider.getInstance(account!!)
        val commands = localStore.pendingCommands
        var processingCommand: PendingCommand? = null
        try {
            for (command in commands) {
                processingCommand = command
                val commandName = command.commandName
                Timber.d("Processing pending command '%s'", commandName)

                /*
                 * We specifically do not catch any exceptions here. If a command fails it is
                 * most likely due to a server or IO error and it must be retried before any
                 * other command processes. This maintains the order of the commands.
                 */try {
                    command.execute(this, account)
                    localStore.removePendingCommand(command)
                    Timber.d("Done processing pending command '%s'", commandName)
                } catch (me: MessagingException) {
                    if (me.isPermanentFailure) {
                        Timber.e(me, "Failure of command '%s' was permanent, removing command from queue", commandName)
                        localStore.removePendingCommand(processingCommand)
                    } else {
                        throw me
                    }
                } catch (e: Exception) {
                    Timber.e(e, "Unexpected exception with command '%s', removing command from queue", commandName)
                    localStore.removePendingCommand(processingCommand)
                    if (K9.DEVELOPER_MODE) {
                        throw AssertionError("Unexpected exception while processing pending command", e)
                    }
                }

                // TODO: When removing a pending command due to an error the local changes should be reverted. Pending
                //  commands that depend on this command should be canceled and local changes be reverted. In most cases
                //  the user should be notified about the failure as well.
            }
        } catch (me: MessagingException) {
            notifyUserIfCertificateProblem(account, me, true)
            Timber.e(me, "Could not process command '%s'", processingCommand)
            throw me
        }
    }

    /**
     * Process a pending append message command. This command uploads a local message to the
     * server, first checking to be sure that the server message is not newer than
     * the local message. Once the local message is successfully processed it is deleted so
     * that the server message will be synchronized down without an additional copy being
     * created.
     */
    @Throws(MessagingException::class)
    fun processPendingAppend(command: PendingAppend, account: Account?) {
        val localStore = localStoreProvider.getInstance(account!!)
        val folderId = command.folderId
        val localFolder = localStore.getFolder(folderId)
        localFolder.open()
        val folderServerId = localFolder.serverId
        val uid = command.uid
        val localMessage = localFolder.getMessage(uid) ?: return
        if (!localMessage.uid.startsWith(K9.LOCAL_UID_PREFIX)) {
            // FIXME: This should never happen. Throw in debug builds.
            return
        }
        val backend = getBackend(account)
        if (localMessage.isSet(Flag.X_REMOTE_COPY_STARTED)) {
            Timber.w("Local message with uid ${localMessage.uid} has flag ${Flag.X_REMOTE_COPY_STARTED} already set, checking for remote message with same message id")
            val messageServerId = backend.findByMessageId(folderServerId, localMessage.messageId)
            if (messageServerId != null) {
                Timber.w("Local message has flag ${Flag.X_REMOTE_COPY_STARTED} already set, and there is a remote message with uid $messageServerId, assuming message was already copied and aborting this copy")
                val oldUid = localMessage.uid
                localMessage.uid = messageServerId
                localFolder.changeUid(localMessage)
                for (l in messagesListeners) {
                    l.messageUidChanged(account, folderId, oldUid, localMessage.uid)
                }
                return
            } else {
                Timber.w("No remote message with message-id found, proceeding with append")
            }
        }

        /*
         * If the message does not exist remotely we just upload it and then
         * update our local copy with the new uid.
         */
        val fp = FetchProfile()
        fp.add(FetchProfile.Item.BODY)
        localFolder.fetch(listOf(localMessage), fp, null)
        val oldUid = localMessage.uid
        localMessage.setFlag(Flag.X_REMOTE_COPY_STARTED, true)
        val messageServerId = backend.uploadMessage(folderServerId, localMessage)
        if (messageServerId == null) {
            // We didn't get the server UID of the uploaded message. Remove the local message now. The uploaded
            // version will be downloaded during the next sync.
            localFolder.destroyMessages(listOf(localMessage))
        } else {
            localMessage.uid = messageServerId
            localFolder.changeUid(localMessage)
            for (l in messagesListeners) {
                l.messageUidChanged(account, folderId, oldUid, localMessage.uid)
            }
        }
    }

    fun processPendingReplace(pendingReplace: PendingReplace?, account: Account?) {
        draftOperations.processPendingReplace(pendingReplace!!, account!!)
    }

    private fun queueMoveOrCopy(
        account: Account, srcFolderId: Long, destFolderId: Long, operation: MoveOrCopyFlavor,
        uidMap: Map<String?, String?>?
    ) {
        val command: PendingCommand
        command = when (operation) {
            MoveOrCopyFlavor.MOVE -> PendingMoveOrCopy.create(srcFolderId, destFolderId, false, uidMap)
            MoveOrCopyFlavor.COPY -> PendingMoveOrCopy.create(srcFolderId, destFolderId, true, uidMap)
            MoveOrCopyFlavor.MOVE_AND_MARK_AS_READ -> PendingMoveAndMarkAsRead.create(srcFolderId, destFolderId, uidMap)
        }
        queuePendingCommand(account, command)
    }

    @Throws(MessagingException::class)
    fun processPendingMoveOrCopy(command: PendingMoveOrCopy, account: Account) {
        val srcFolder = command.srcFolderId
        val destFolder = command.destFolderId
        val operation = if (command.isCopy) MoveOrCopyFlavor.COPY else MoveOrCopyFlavor.MOVE
        val newUidMap = command.newUidMap
        val uids = if (newUidMap != null) ArrayList(newUidMap.keys) else command.uids
        processPendingMoveOrCopy(account, srcFolder, destFolder, uids, operation, newUidMap)
    }

    @Throws(MessagingException::class)
    fun processPendingMoveAndRead(command: PendingMoveAndMarkAsRead, account: Account) {
        val srcFolder = command.srcFolderId
        val destFolder = command.destFolderId
        val newUidMap = command.newUidMap
        val uids: List<String> = ArrayList(newUidMap.keys)
        processPendingMoveOrCopy(
            account, srcFolder, destFolder, uids,
            MoveOrCopyFlavor.MOVE_AND_MARK_AS_READ, newUidMap
        )
    }

    @VisibleForTesting
    @Throws(MessagingException::class)
    fun processPendingMoveOrCopy(
        account: Account, srcFolderId: Long, destFolderId: Long, uids: List<String>,
        operation: MoveOrCopyFlavor, newUidMap: Map<String, String?>?
    ) {
        Preconditions.checkNotNull(newUidMap)
        val localStore = localStoreProvider.getInstance(account)
        val localSourceFolder = localStore.getFolder(srcFolderId)
        localSourceFolder.open()
        val srcFolderServerId = localSourceFolder.serverId
        val localDestFolder = localStore.getFolder(destFolderId)
        localDestFolder.open()
        val destFolderServerId = localDestFolder.serverId
        val backend = getBackend(account)
        var remoteUidMap: Map<String, String>?
        when (operation) {
            MoveOrCopyFlavor.COPY -> remoteUidMap = backend.copyMessages(srcFolderServerId, destFolderServerId, uids)
            MoveOrCopyFlavor.MOVE -> remoteUidMap = backend.moveMessages(srcFolderServerId, destFolderServerId, uids)
            MoveOrCopyFlavor.MOVE_AND_MARK_AS_READ -> remoteUidMap =
                backend.moveMessagesAndMarkAsRead(srcFolderServerId, destFolderServerId, uids)
        }
        if (operation != MoveOrCopyFlavor.COPY) {
            if (backend.supportsExpunge && account.expungePolicy == Expunge.EXPUNGE_IMMEDIATELY) {
                Timber.i("processingPendingMoveOrCopy expunging folder %s:%s", account.description, srcFolderServerId)
                backend.expungeMessages(srcFolderServerId, uids)
            }
            destroyPlaceholderMessages(localSourceFolder, uids)
        }

        // TODO: Change Backend interface to ensure we never receive null for remoteUidMap
        if (remoteUidMap == null) {
            remoteUidMap = emptyMap()
        }

        // Update local messages (that currently have local UIDs) with new server IDs
        for (uid in uids) {
            val localUid = newUidMap!![uid]
            val newUid = remoteUidMap[uid]
            val localMessage = localDestFolder.getMessage(localUid) ?: continue
            if (newUid != null) {
                // Update local message with new server ID
                localMessage.uid = newUid
                localDestFolder.changeUid(localMessage)
                for (l in messagesListeners) {
                    l.messageUidChanged(account, destFolderId, localUid, newUid)
                }
            } else {
                // New server ID wasn't provided. Remove local message.
                localMessage.destroy()
            }
        }
    }

    @Throws(MessagingException::class)
    fun destroyPlaceholderMessages(localFolder: LocalFolder, uids: List<String>) {
        for (uid in uids) {
            val placeholderMessage = localFolder.getMessage(uid) ?: continue
            if (placeholderMessage.isSet(Flag.DELETED)) {
                placeholderMessage.destroy()
            } else {
                Timber.w(
                    "Expected local message %s in folder %s to be a placeholder, but DELETE flag wasn't set",
                    uid, localFolder.serverId
                )
                if (BuildConfig.DEBUG) {
                    throw AssertionError("Placeholder message must have the DELETED flag set")
                }
            }
        }
    }

    private fun queueSetFlag(account: Account, folderId: Long, newState: Boolean, flag: Flag, uids: List<String>) {
        putBackground("queueSetFlag", null) {
            val command: PendingCommand = PendingSetFlag.create(folderId, newState, flag, uids)
            queuePendingCommand(account, command)
            processPendingCommands(account)
        }
    }

    /**
     * Processes a pending mark read or unread command.
     */
    @Throws(MessagingException::class)
    fun processPendingSetFlag(command: PendingSetFlag, account: Account) {
        val backend = getBackend(account)
        val folderServerId = getFolderServerId(account, command.folderId)
        backend.setFlag(folderServerId, command.uids, command.flag, command.newState)
    }

    private fun queueDelete(account: Account, folderId: Long, uids: List<String>) {
        putBackground("queueDelete", null) {
            val command: PendingCommand = PendingDelete.create(folderId, uids)
            queuePendingCommand(account, command)
            processPendingCommands(account)
        }
    }

    @Throws(MessagingException::class)
    fun processPendingDelete(command: PendingDelete, account: Account) {
        val folderId = command.folderId
        val uids = command.uids
        val backend = getBackend(account)
        val folderServerId = getFolderServerId(account, folderId)
        backend.deleteMessages(folderServerId, uids)
        val localStore = localStoreProvider.getInstance(account)
        val localFolder = localStore.getFolder(folderId)
        localFolder.open()
        destroyPlaceholderMessages(localFolder, uids)
    }

    private fun queueExpunge(account: Account, folderId: Long) {
        val command: PendingCommand = PendingExpunge.create(folderId)
        queuePendingCommand(account, command)
    }

    @Throws(MessagingException::class)
    fun processPendingExpunge(command: PendingExpunge, account: Account) {
        val backend = getBackend(account)
        val folderServerId = getFolderServerId(account, command.folderId)
        backend.expunge(folderServerId)
    }

    @Throws(MessagingException::class)
    fun processPendingMarkAllAsRead(command: PendingMarkAllAsRead, account: Account?) {
        val folderId = command.folderId
        val localStore = localStoreProvider.getInstance(account!!)
        val localFolder = localStore.getFolder(folderId)
        localFolder.open()
        val folderServerId = localFolder.serverId
        Timber.i("Marking all messages in %s:%s as read", account, folderServerId)

        // TODO: Make this one database UPDATE operation
        val messages = localFolder.getMessages(null, false)
        for (message in messages) {
            if (!message.isSet(Flag.SEEN)) {
                message.setFlag(Flag.SEEN, true)
            }
        }
        for (l in messagesListeners) {
            l.folderStatusChanged(account, folderId)
        }
        val backend = getBackend(account)
        if (backend.supportsFlags) {
            backend.markAllAsRead(folderServerId)
        }
    }

    fun markAllMessagesRead(account: Account?, folderId: Long) {
        val command: PendingCommand = PendingMarkAllAsRead.create(folderId)
        queuePendingCommand(account, command)
        processPendingCommands(account)
    }

    fun setFlag(
        account: Account, messageIds: List<Long>, flag: Flag,
        newState: Boolean
    ) {
        setFlagInCache(account, messageIds, flag, newState)
        threadPool.execute { setFlagSynchronous(account, messageIds, flag, newState, false) }
    }

    fun setFlagForThreads(
        account: Account, threadRootIds: List<Long>,
        flag: Flag, newState: Boolean
    ) {
        setFlagForThreadsInCache(account, threadRootIds, flag, newState)
        threadPool.execute { setFlagSynchronous(account, threadRootIds, flag, newState, true) }
    }

    private fun setFlagSynchronous(
        account: Account, ids: List<Long>,
        flag: Flag, newState: Boolean, threadedList: Boolean
    ) {
        val localStore: LocalStore
        localStore = try {
            localStoreProvider.getInstance(account)
        } catch (e: MessagingException) {
            Timber.e(e, "Couldn't get LocalStore instance")
            return
        }

        // Update affected messages in the database. This should be as fast as possible so the UI
        // can be updated with the new state.
        try {
            if (threadedList) {
                localStore.setFlagForThreads(ids, flag, newState)
                removeFlagForThreadsFromCache(account, ids, flag)
            } else {
                localStore.setFlag(ids, flag, newState)
                removeFlagFromCache(account, ids, flag)
            }
        } catch (e: MessagingException) {
            Timber.e(e, "Couldn't set flags in local database")
        }

        // Read folder ID and UID of messages from the database
        val folderMap: Map<Long, List<String>>
        folderMap = try {
            localStore.getFolderIdsAndUids(ids, threadedList)
        } catch (e: MessagingException) {
            Timber.e(e, "Couldn't get folder name and UID of messages")
            return
        }
        val accountSupportsFlags = supportsFlags(account)

        // Loop over all folders
        for ((folderId, uids) in folderMap) {

            // Notify listeners of changed folder status
            for (l in messagesListeners) {
                l.folderStatusChanged(account, folderId)
            }
            if (accountSupportsFlags) {
                val localFolder = localStore.getFolder(folderId)
                try {
                    localFolder.open()
                    if (!localFolder.isLocalOnly) {
                        // Send flag change to server
                        queueSetFlag(account, folderId, newState, flag, uids)
                        processPendingCommands(account)
                    }
                } catch (e: MessagingException) {
                    Timber.e(e, "Couldn't open folder. Account: %s, folder ID: %d", account, folderId)
                }
            }
        }
    }

    /**
     * Set or remove a flag for a set of messages in a specific folder.
     *
     *
     * The [Message] objects passed in are updated to reflect the new flag state.
     *
     */
    fun setFlag(account: Account, folderId: Long, messages: List<LocalMessage>, flag: Flag, newState: Boolean) {
        // TODO: Put this into the background, but right now some callers depend on the message
        //       objects being modified right after this method returns.
        try {
            val localStore = localStoreProvider.getInstance(account)
            val localFolder = localStore.getFolder(folderId)
            localFolder.open()

            // Update the messages in the local store
            localFolder.setFlags(messages, setOf(flag), newState)
            for (l in messagesListeners) {
                l.folderStatusChanged(account, folderId)
            }

            // Handle the remote side
            if (supportsFlags(account) && !localFolder.isLocalOnly) {
                val uids = getUidsFromMessages(messages)
                queueSetFlag(account, folderId, newState, flag, uids)
                processPendingCommands(account)
            }
        } catch (me: MessagingException) {
            throw RuntimeException(me)
        }
    }

    /**
     * Set or remove a flag for a message referenced by message UID.
     */
    fun setFlag(account: Account?, folderId: Long, uid: String?, flag: Flag?, newState: Boolean) {
        try {
            val localStore = localStoreProvider.getInstance(account!!)
            val localFolder = localStore.getFolder(folderId)
            localFolder.open()
            val message = localFolder.getMessage(uid)
            if (message != null) {
                setFlag(account, folderId, listOf(message), flag!!, newState)
            }
        } catch (me: MessagingException) {
            throw RuntimeException(me)
        }
    }

    fun clearAllPending(account: Account?) {
        try {
            Timber.w("Clearing pending commands!")
            val localStore = localStoreProvider.getInstance(account!!)
            localStore.removePendingCommands()
        } catch (me: MessagingException) {
            Timber.e(me, "Unable to clear pending command")
        }
    }

    fun loadMessageRemotePartial(account: Account, folderId: Long, uid: String, listener: MessagingListener) {
        put(
            "loadMessageRemotePartial", listener
        ) { loadMessageRemoteSynchronous(account, folderId, uid, listener, true) }
    }

    // TODO: Fix the callback mess. See GH-782
    fun loadMessageRemote(account: Account, folderId: Long, uid: String, listener: MessagingListener) {
        put(
            "loadMessageRemote", listener
        ) { loadMessageRemoteSynchronous(account, folderId, uid, listener, false) }
    }

    private fun loadMessageRemoteSynchronous(
        account: Account, folderId: Long, uid: String,
        listener: MessagingListener, loadPartialFromSearch: Boolean
    ) {
        try {
            val localStore = localStoreProvider.getInstance(account)
            val localFolder = localStore.getFolder(folderId)
            localFolder.open()
            val folderServerId = localFolder.serverId
            var message = localFolder.getMessage(uid)
            if (uid.startsWith(K9.LOCAL_UID_PREFIX)) {
                Timber.w("Message has local UID so cannot download fully.")
                // ASH move toast
                Toast.makeText(
                    context,
                    "Message has local UID so cannot download fully",
                    Toast.LENGTH_LONG
                ).show()
                // TODO: Using X_DOWNLOADED_FULL is wrong because it's only a partial message. But
                // one we can't download completely. Maybe add a new flag; X_PARTIAL_MESSAGE ?
                message.setFlag(Flag.X_DOWNLOADED_FULL, true)
                message.setFlag(Flag.X_DOWNLOADED_PARTIAL, false)
            } else {
                val backend = getBackend(account)
                if (loadPartialFromSearch) {
                    val syncConfig = createSyncConfig(account)
                    backend.downloadMessage(syncConfig, folderServerId, uid)
                } else {
                    backend.downloadCompleteMessage(folderServerId, uid)
                }
                message = localFolder.getMessage(uid)
                if (!loadPartialFromSearch) {
                    message.setFlag(Flag.X_DOWNLOADED_FULL, true)
                }
            }

            // now that we have the full message, refresh the headers
            for (l in getListeners(listener)) {
                l.loadMessageRemoteFinished(account, folderId, uid)
            }
        } catch (e: Exception) {
            for (l in getListeners(listener)) {
                l.loadMessageRemoteFailed(account, folderId, uid, e)
            }
            notifyUserIfCertificateProblem(account, e, true)
            Timber.e(e, "Error while loading remote message")
        }
    }

    @Throws(MessagingException::class)
    fun loadMessage(account: Account, folderId: Long, uid: String): LocalMessage {
        val localStore = localStoreProvider.getInstance(account)
        val localFolder = localStore.getFolder(folderId)
        localFolder.open()
        val message = localFolder.getMessage(uid)
        if (message == null || message.databaseId == 0L) {
            val folderName = localFolder.name
            throw IllegalArgumentException("Message not found: folder=$folderName, uid=$uid")
        }
        val fp = FetchProfile()
        fp.add(FetchProfile.Item.BODY)
        localFolder.fetch(listOf(message), fp, null)
        notificationController.removeNewMailNotification(account, message.makeMessageReference())
        markMessageAsReadOnView(account, message)
        return message
    }

    @Throws(MessagingException::class)
    fun loadMessageMetadata(account: Account?, folderId: Long, uid: String): LocalMessage {
        val localStore = localStoreProvider.getInstance(account!!)
        val localFolder = localStore.getFolder(folderId)
        localFolder.open()
        val message = localFolder.getMessage(uid)
        if (message == null || message.databaseId == 0L) {
            val folderName = localFolder.name
            throw IllegalArgumentException("Message not found: folder=$folderName, uid=$uid")
        }
        val fp = FetchProfile()
        fp.add(FetchProfile.Item.ENVELOPE)
        localFolder.fetch(listOf(message), fp, null)
        return message
    }

    @Throws(MessagingException::class)
    private fun markMessageAsReadOnView(account: Account, message: LocalMessage) {
        if (account.isMarkMessageAsReadOnView && !message.isSet(Flag.SEEN)) {
            val messageIds = listOf(message.databaseId)
            setFlag(account, messageIds, Flag.SEEN, true)
            message.setFlagInternal(Flag.SEEN, true)
        }
    }

    fun loadAttachment(
        account: Account?, message: LocalMessage, part: Part?,
        listener: MessagingListener
    ) {
        put("loadAttachment", listener) {
            try {
                val folderServerId = message.folder.serverId
                val localStore = localStoreProvider.getInstance(account!!)
                val localFolder = localStore.getFolder(folderServerId)
                val bodyFactory = ProgressBodyFactory { progress ->
                    messagesListeners.forEach {
                        it.updateProgress(progress)
                    }
                }
                val backend = getBackend(account)
                backend.fetchPart(folderServerId, message.uid, part!!, bodyFactory)
                localFolder.addPartToMessage(message, part)
                for (l in getListeners(listener)) {
                    l.loadAttachmentFinished(account, message, part)
                }
            } catch (me: MessagingException) {
                Timber.v(me, "Exception loading attachment")
                for (l in getListeners(listener)) {
                    l.loadAttachmentFailed(account, message, part, me.message)
                }
                notifyUserIfCertificateProblem(account, me, true)
            }
        }
    }

    /**
     * Stores the given message in the Outbox and starts a sendPendingMessages command to attempt to send the message.
     */
    fun sendMessage(account: Account, message: Message, plaintextSubject: String?, listener: MessagingListener?) {
        try {
            val outboxFolderId = account.outboxFolderId
            if (outboxFolderId == null) {
                Timber.e("Error sending message. No Outbox folder configured.")
                return
            }
            message.setFlag(Flag.SEEN, true)
            val messageStore = messageStoreManager.getMessageStore(account)
            val messageData = saveMessageDataCreator.createSaveMessageData(message, false, plaintextSubject)
            val messageId = messageStore.saveLocalMessage(outboxFolderId, messageData, null)
            val localStore = localStoreProvider.getInstance(account)
            val outboxStateRepository = localStore.outboxStateRepository
            outboxStateRepository.initializeOutboxState(messageId)
            sendPendingMessages(account, listener)
        } catch (e: Exception) {
            Timber.e(e, "Error sending message")
        }
    }

    @Throws(MessagingException::class)
    fun sendMessageBlocking(account: Account?, message: Message?) {
        val backend = getBackend(account)
        backend.sendMessage(message!!)
    }

    fun sendPendingMessages(listener: MessagingListener?) {
        for (account in preferences.availableAccounts) {
            sendPendingMessages(account, listener)
        }
    }

    /**
     * Attempt to send any messages that are sitting in the Outbox.
     */
    fun sendPendingMessages(
        account: Account,
        listener: MessagingListener?
    ) {
        putBackground("sendPendingMessages", listener) {
            if (!account.isAvailable(context)) {
                throw UnavailableAccountException()
            }
            if (messagesPendingSend(account)) {
                showSendingNotificationIfNecessary(account)
                try {
                    sendPendingMessagesSynchronous(account)
                } finally {
                    clearSendingNotificationIfNecessary(account)
                }
            }
        }
    }

    private fun showSendingNotificationIfNecessary(account: Account) {
        if (account.isNotifySync) {
            notificationController.showSendingNotification(account)
        }
    }

    private fun clearSendingNotificationIfNecessary(account: Account) {
        if (account.isNotifySync) {
            notificationController.clearSendingNotification(account)
        }
    }

    private fun messagesPendingSend(account: Account): Boolean {
        try {
            val localFolder = localStoreProvider.getInstance(account).getFolder(account.outboxFolderId!!)
            if (!localFolder.exists()) {
                return false
            }
            localFolder.open()
            if (localFolder.messageCount > 0) {
                return true
            }
        } catch (e: Exception) {
            Timber.e(e, "Exception while checking for unsent messages")
        }
        return false
    }

    /**
     * Attempt to send any messages that are sitting in the Outbox.
     */
    @VisibleForTesting
    fun sendPendingMessagesSynchronous(account: Account) {
        var lastFailure: Exception? = null
        var wasPermanentFailure = false
        try {
            val localStore = localStoreProvider.getInstance(account)
            val outboxStateRepository = localStore.outboxStateRepository
            val localFolder = localStore.getFolder(account.outboxFolderId!!)
            if (!localFolder.exists()) {
                Timber.v("Outbox does not exist")
                return
            }
            localFolder.open()
            val outboxFolderId = localFolder.databaseId
            val localMessages = localFolder.getMessages(null)
            var progress = 0
            val todo = localMessages.size
            for (l in messagesListeners) {
                l.synchronizeMailboxProgress(account, outboxFolderId, progress, todo)
            }
            /*
             * The profile we will use to pull all of the content
             * for a given local message into memory for sending.
             */
            val fp = FetchProfile()
            fp.add(FetchProfile.Item.ENVELOPE)
            fp.add(FetchProfile.Item.BODY)
            Timber.i("Scanning Outbox folder for messages to send")
            val backend = getBackend(account)
            for (message in localMessages) {
                if (message.isSet(Flag.DELETED)) {
                    // FIXME: When uploading a message to the remote Sent folder the move code creates a placeholder
                    // message in the Outbox. This code gets rid of these messages. It'd be preferable if the
                    // placeholder message was never created, though.
                    message.destroy()
                    continue
                }
                try {
                    val messageId = message.databaseId
                    val (sendState, numberOfSendAttempts) = outboxStateRepository.getOutboxState(messageId)
                    if (sendState !== SendState.READY) {
                        Timber.v("Skipping sending message ${message.uid}")
                        notificationController.showSendFailedNotification(
                            account,
                            MessagingException(message.subject)
                        )
                        continue
                    }
                    Timber.i(
                        "Send count for message %s is %d", message.uid,
                        numberOfSendAttempts
                    )
                    localFolder.fetch(listOf(message), fp, null)
                    try {
                        if (message.getHeader(K9.IDENTITY_HEADER).isNotEmpty() || message.isSet(Flag.DRAFT)) {
                            Timber.v("The user has set the Outbox and Drafts folder to the same thing.  This message appears to be a draft, so K-9 will not send it")
                            continue
                        }
                        outboxStateRepository.incrementSendAttempts(messageId)
                        message.setFlag(Flag.X_SEND_IN_PROGRESS, true)
                        Timber.i("Sending message with UID %s", message.uid)
                        backend.sendMessage(message)
                        message.setFlag(Flag.X_SEND_IN_PROGRESS, false)
                        message.setFlag(Flag.SEEN, true)
                        progress++
                        for (l in messagesListeners) {
                            l.synchronizeMailboxProgress(account, outboxFolderId, progress, todo)
                        }
                        moveOrDeleteSentMessage(account, localStore, message)
                        outboxStateRepository.removeOutboxState(messageId)
                    } catch (e: AuthenticationFailedException) {
                        outboxStateRepository.decrementSendAttempts(messageId)
                        lastFailure = e
                        wasPermanentFailure = false
                        handleAuthenticationFailure(account, false)
                        handleSendFailure(account, localFolder, message, e)
                    } catch (e: CertificateValidationException) {
                        outboxStateRepository.decrementSendAttempts(messageId)
                        lastFailure = e
                        wasPermanentFailure = false
                        notifyUserIfCertificateProblem(account, e, false)
                        handleSendFailure(account, localFolder, message, e)
                    } catch (e: MessagingException) {
                        lastFailure = e
                        wasPermanentFailure = e.isPermanentFailure
                        if (wasPermanentFailure) {
                            val errorMessage = e.message
                            outboxStateRepository.setSendAttemptError(messageId, errorMessage!!)
                        } else if (numberOfSendAttempts + 1 >= K9.MAX_SEND_ATTEMPTS) {
                            outboxStateRepository.setSendAttemptsExceeded(messageId)
                        }
                        handleSendFailure(account, localFolder, message, e)
                    } catch (e: Exception) {
                        lastFailure = e
                        wasPermanentFailure = true
                        handleSendFailure(account, localFolder, message, e)
                    }
                } catch (e: Exception) {
                    lastFailure = e
                    wasPermanentFailure = false
                    Timber.e(e, "Failed to fetch message for sending")
                    notifySynchronizeMailboxFailed(account, localFolder, e)
                }
            }
            if (lastFailure != null) {
                if (wasPermanentFailure) {
                    notificationController.showSendFailedNotification(account, lastFailure)
                } else {
                    notificationController.showSendFailedNotification(account, lastFailure)
                }
            }
        } catch (e: UnavailableStorageException) {
            Timber.i("Failed to send pending messages because storage is not available - trying again later.")
            throw UnavailableAccountException(e)
        } catch (e: Exception) {
            Timber.v(e, "Failed to send pending messages")
        } finally {
            if (lastFailure == null) {
                notificationController.clearSendFailedNotification(account)
            }
        }
    }

    @Throws(MessagingException::class)
    private fun moveOrDeleteSentMessage(account: Account, localStore: LocalStore, message: LocalMessage) {
        if (!account.hasSentFolder() || !account.isUploadSentMessages) {
            Timber.i("Not uploading sent message; deleting local message")
            message.destroy()
        } else {
            val sentFolderId = account.sentFolderId!!
            val sentFolder = localStore.getFolder(sentFolderId)
            sentFolder.open()
            val sentFolderServerId = sentFolder.serverId
            Timber.i("Moving sent message to folder '%s' (%d)", sentFolderServerId, sentFolderId)
            val messageStore = messageStoreManager.getMessageStore(account)
            val destinationMessageId = messageStore.moveMessage(message.databaseId, sentFolderId)
            Timber.i("Moved sent message to folder '%s' (%d)", sentFolderServerId, sentFolderId)
            if (!sentFolder.isLocalOnly) {
                val destinationUid = messageStore.getMessageServerId(destinationMessageId)
                val command: PendingCommand = PendingAppend.create(sentFolderId, destinationUid)
                queuePendingCommand(account, command)
                processPendingCommands(account)
            }
        }
    }

    @Throws(MessagingException::class)
    private fun handleSendFailure(account: Account, localFolder: LocalFolder, message: Message, exception: Exception) {
        Timber.e(exception, "Failed to send message")
        message.setFlag(Flag.X_SEND_FAILED, true)
        notifySynchronizeMailboxFailed(account, localFolder, exception)
    }

    private fun notifySynchronizeMailboxFailed(account: Account, localFolder: LocalFolder, exception: Exception) {
        val folderId = localFolder.databaseId
        val errorMessage = ExceptionHelper.getRootCauseMessage(exception)
        for (listener in messagesListeners) {
            listener.synchronizeMailboxFailed(account, folderId, errorMessage)
        }
    }

    fun getUnreadMessageCount(account: Account?): Int {
        return unreadMessageCountProvider.getUnreadMessageCount(account!!)
    }

    fun getUnreadMessageCount(searchAccount: SearchAccount?): Int {
        return unreadMessageCountProvider.getUnreadMessageCount(searchAccount!!)
    }

    @Throws(MessagingException::class)
    fun getFolderUnreadMessageCount(account: Account?, folderId: Long?): Int {
        val localStore = localStoreProvider.getInstance(account!!)
        val localFolder = localStore.getFolder(folderId!!)
        return localFolder.unreadMessageCount
    }

    fun isMoveCapable(messageReference: MessageReference): Boolean {
        return !messageReference.uid.startsWith(K9.LOCAL_UID_PREFIX)
    }

    fun isCopyCapable(message: MessageReference): Boolean {
        return isMoveCapable(message)
    }

    fun isMoveCapable(account: Account?): Boolean {
        return getBackend(account).supportsMove
    }

    fun isCopyCapable(account: Account?): Boolean {
        return getBackend(account).supportsCopy
    }

    fun isPushCapable(account: Account?): Boolean {
        return getBackend(account).isPushCapable
    }

    fun supportsFlags(account: Account?): Boolean {
        return getBackend(account).supportsFlags
    }

    fun supportsExpunge(account: Account?): Boolean {
        return getBackend(account).supportsExpunge
    }

    fun supportsSearchByDate(account: Account?): Boolean {
        return getBackend(account).supportsSearchByDate
    }

    fun supportsUpload(account: Account?): Boolean {
        return getBackend(account).supportsUpload
    }

    @Throws(MessagingException::class)
    fun checkIncomingServerSettings(account: Account?) {
        getBackend(account).checkIncomingServerSettings()
    }

    @Throws(MessagingException::class)
    fun checkOutgoingServerSettings(account: Account?) {
        getBackend(account).checkOutgoingServerSettings()
    }

    fun moveMessages(
        srcAccount: Account?, srcFolderId: Long,
        messageReferences: List<MessageReference>, destFolderId: Long
    ) {
        actOnMessageGroup(
            srcAccount,
            srcFolderId,
            messageReferences,
            object : MessageActor {
                override fun act(account: Account, messageFolder: LocalFolder, messages: List<LocalMessage>) {
                    suppressMessages(account, messages)
                    putBackground("moveMessages", null) {
                        moveOrCopyMessageSynchronous(
                            account, srcFolderId, messages, destFolderId,
                            MoveOrCopyFlavor.MOVE
                        )
                    }
                }
            })
    }

    fun moveMessagesInThread(
        srcAccount: Account, srcFolderId: Long,
        messageReferences: List<MessageReference>, destFolderId: Long
    ) {
        actOnMessageGroup(
            srcAccount,
            srcFolderId,
            messageReferences,
            object : MessageActor {
                override fun act(account: Account, messageFolder: LocalFolder, messages: List<LocalMessage>) {
                    suppressMessages(account, messages)
                    putBackground("moveMessagesInThread", null) {
                        try {
                            val messagesInThreads = collectMessagesInThreads(account, messages)
                            moveOrCopyMessageSynchronous(
                                account, srcFolderId, messagesInThreads, destFolderId,
                                MoveOrCopyFlavor.MOVE
                            )
                        } catch (e: MessagingException) {
                            Timber.e(e, "Exception while moving messages")
                        }
                    }
                }
            })
    }

    fun moveMessage(account: Account, srcFolderId: Long, message: MessageReference, destFolderId: Long) {
        moveMessages(account, srcFolderId, listOf(message), destFolderId)
    }

    fun copyMessages(
        srcAccount: Account, srcFolderId: Long,
        messageReferences: List<MessageReference>, destFolderId: Long
    ) {
        actOnMessageGroup(
            srcAccount,
            srcFolderId,
            messageReferences,
            object : MessageActor {
                override fun act(account: Account, messageFolder: LocalFolder, messages: List<LocalMessage>) {
                    putBackground(
                        "copyMessages", null
                    ) {
                        moveOrCopyMessageSynchronous(
                            srcAccount,
                            srcFolderId,
                            messages,
                            destFolderId,
                            MoveOrCopyFlavor.COPY
                        )
                    }
                }
            })
    }

    fun copyMessagesInThread(
        srcAccount: Account?, srcFolderId: Long,
        messageReferences: List<MessageReference>, destFolderId: Long
    ) {
        actOnMessageGroup(
            srcAccount,
            srcFolderId,
            messageReferences,
            object : MessageActor {
                override fun act(account: Account, messageFolder: LocalFolder, messages: List<LocalMessage>) {
                    putBackground("copyMessagesInThread", null) {
                        try {
                            val messagesInThreads = collectMessagesInThreads(account, messages)
                            moveOrCopyMessageSynchronous(
                                account, srcFolderId, messagesInThreads, destFolderId,
                                MoveOrCopyFlavor.COPY
                            )
                        } catch (e: MessagingException) {
                            Timber.e(e, "Exception while copying messages")
                        }
                    }
                }
            })
    }

    fun copyMessage(account: Account, srcFolderId: Long, message: MessageReference, destFolderId: Long) {
        copyMessages(account, srcFolderId, listOf(message), destFolderId)
    }

    private fun moveOrCopyMessageSynchronous(
        account: Account, srcFolderId: Long, inMessages: List<LocalMessage>,
        destFolderId: Long, operation: MoveOrCopyFlavor
    ) {
        if (operation == MoveOrCopyFlavor.MOVE_AND_MARK_AS_READ) {
            throw UnsupportedOperationException("MOVE_AND_MARK_AS_READ unsupported")
        }
        try {
            val localStore = localStoreProvider.getInstance(account)
            if (operation == MoveOrCopyFlavor.MOVE && !isMoveCapable(account)) {
                return
            }
            if (operation == MoveOrCopyFlavor.COPY && !isCopyCapable(account)) {
                return
            }
            val localSrcFolder = localStore.getFolder(srcFolderId)
            localSrcFolder.open()
            val localDestFolder = localStore.getFolder(destFolderId)
            localDestFolder.open()
            var unreadCountAffected = false
            val uids: MutableList<String> = LinkedList()
            for (message in inMessages) {
                val uid = message.uid
                if (!uid.startsWith(K9.LOCAL_UID_PREFIX)) {
                    uids.add(uid)
                }
                if (!unreadCountAffected && !message.isSet(Flag.SEEN)) {
                    unreadCountAffected = true
                }
            }
            val messages = localSrcFolder.getMessagesByUids(uids)
            if (messages.size > 0) {
                val messageStore = messageStoreManager.getMessageStore(account)
                val messageIds: MutableList<Long> = ArrayList()
                val messageIdToUidMapping: MutableMap<Long, String> = HashMap()
                for (message in messages) {
                    val messageId = message.databaseId
                    messageIds.add(messageId)
                    messageIdToUidMapping[messageId] = message.uid
                }
                val resultIdMapping: Map<Long, Long>
                if (operation == MoveOrCopyFlavor.COPY) {
                    resultIdMapping = messageStore.copyMessages(messageIds, destFolderId)
                    if (unreadCountAffected) {
                        // If this copy operation changes the unread count in the destination
                        // folder, notify the listeners.
                        for (l in messagesListeners) {
                            l.folderStatusChanged(account, destFolderId)
                        }
                    }
                } else {
                    resultIdMapping = messageStore.moveMessages(messageIds, destFolderId)
                    unsuppressMessages(account, messages)
                    if (unreadCountAffected) {
                        // If this move operation changes the unread count, notify the listeners
                        // that the unread count changed in both the source and destination folder.
                        for (l in messagesListeners) {
                            l.folderStatusChanged(account, srcFolderId)
                            l.folderStatusChanged(account, destFolderId)
                        }
                    }
                }
                val destinationMapping = messageStore.getMessageServerIds(resultIdMapping.values)
                val uidMap: MutableMap<String?, String?> = HashMap()
                for ((sourceMessageId, destinationMessageId) in resultIdMapping) {
                    val sourceUid = messageIdToUidMapping[sourceMessageId]
                    val destinationUid = destinationMapping[destinationMessageId]
                    uidMap[sourceUid] = destinationUid
                }
                queueMoveOrCopy(
                    account, localSrcFolder.databaseId, localDestFolder.databaseId,
                    operation, uidMap
                )
            }
            processPendingCommands(account)
        } catch (e: UnavailableStorageException) {
            Timber.i("Failed to move/copy message because storage is not available - trying again later.")
            throw UnavailableAccountException(e)
        } catch (me: MessagingException) {
            throw RuntimeException("Error moving message", me)
        }
    }

    fun moveToDraftsFolder(account: Account, folderId: Long, messages: List<MessageReference>) {
        putBackground("moveToDrafts", null) { moveToDraftsFolderInBackground(account, folderId, messages) }
    }

    private fun moveToDraftsFolderInBackground(account: Account, folderId: Long, messages: List<MessageReference>) {
        for (messageReference in messages) {
            try {
                val message: Message = loadMessage(account, folderId, messageReference.uid)
                val draftMessageId = saveDraft(account, message, null, message.subject)
                val draftSavedSuccessfully = draftMessageId != null
                if (draftSavedSuccessfully) {
                    message.destroy()
                }
                for (listener in messagesListeners) {
                    listener.folderStatusChanged(account, folderId)
                }
            } catch (e: MessagingException) {
                Timber.e(e, "Error loading message. Draft was not saved.")
            }
        }
    }

    fun expunge(account: Account, folderId: Long) {
        putBackground("expunge", null) {
            queueExpunge(account, folderId)
            processPendingCommands(account)
        }
    }

    fun deleteDraft(account: Account, id: Long) {
        try {
            val folderId = account.draftsFolderId
            if (folderId == null) {
                Timber.w("No Drafts folder configured. Can't delete draft.")
                return
            }
            val localStore = localStoreProvider.getInstance(account)
            val localFolder = localStore.getFolder(folderId)
            localFolder.open()
            val uid = localFolder.getMessageUidById(id)
            if (uid != null) {
                val messageReference = MessageReference(account.uuid, folderId, uid, null)
                deleteMessage(messageReference)
            }
        } catch (me: MessagingException) {
            Timber.e(me, "Error deleting draft")
        }
    }

    fun deleteThreads(messages: List<MessageReference>) {
        actOnMessagesGroupedByAccountAndFolder(
            messages,
            object : MessageActor {
                override fun act(account: Account, messageFolder: LocalFolder, messages: List<LocalMessage>) {
                    suppressMessages(account, messages)
                    putBackground(
                        "deleteThreads", null
                    ) { deleteThreadsSynchronous(account, messageFolder.databaseId, messages) }
                }
            })
    }

    private fun deleteThreadsSynchronous(account: Account, folderId: Long, messages: List<LocalMessage>) {
        try {
            val messagesToDelete = collectMessagesInThreads(account, messages)
            deleteMessagesSynchronous(account, folderId, messagesToDelete)
        } catch (e: MessagingException) {
            Timber.e(e, "Something went wrong while deleting threads")
        }
    }

    @Throws(MessagingException::class)
    private fun collectMessagesInThreads(account: Account, messages: List<LocalMessage>): List<LocalMessage> {
        val localStore = localStoreProvider.getInstance(account)
        val messagesInThreads: MutableList<LocalMessage> = ArrayList()
        for (localMessage in messages) {
            val rootId = localMessage.rootId
            val threadId = if (rootId == -1L) localMessage.threadId else rootId
            val messagesInThread = localStore.getMessagesInThread(threadId)
            messagesInThreads.addAll(messagesInThread)
        }
        return messagesInThreads
    }

    fun deleteMessage(message: MessageReference) {
        deleteMessages(listOf(message))
    }

    fun deleteMessages(messages: List<MessageReference>) {
        actOnMessagesGroupedByAccountAndFolder(
            messages,
            object : MessageActor {
                override fun act(account: Account, messageFolder: LocalFolder, messages: List<LocalMessage>) {
                    suppressMessages(account, messages)
                    putBackground(
                        "deleteMessages", null
                    ) { deleteMessagesSynchronous(account, messageFolder.databaseId, messages) }
                }
            })
    }

    @SuppressLint("NewApi") // used for debugging only
    fun debugClearMessagesLocally(messages: List<MessageReference>) {
        if (!K9.DEVELOPER_MODE) {
            throw AssertionError("method must only be used in developer mode!")
        }
        actOnMessagesGroupedByAccountAndFolder(messages, object : MessageActor {
            override fun act(account: Account, messageFolder: LocalFolder, messages: List<LocalMessage>) {
                putBackground("debugClearLocalMessages", null) {
                    for (message in messages) {
                        try {
                            message.debugClearLocalData()
                        } catch (e: MessagingException) {
                            throw AssertionError("clearing local message content failed!", e)
                        }
                    }
                }
            }
        })
    }

    private fun deleteMessagesSynchronous(account: Account, folderId: Long, messages: List<LocalMessage>) {
        try {
            val localOnlyMessages: MutableList<LocalMessage> = ArrayList()
            val syncedMessages: MutableList<LocalMessage> = ArrayList()
            val syncedMessageUids: MutableList<String> = ArrayList()
            for (message in messages) {
                val uid = message.uid
                if (uid.startsWith(K9.LOCAL_UID_PREFIX)) {
                    localOnlyMessages.add(message)
                } else {
                    syncedMessages.add(message)
                    syncedMessageUids.add(uid)
                }
            }
            val backend = getBackend(account)
            val localStore = localStoreProvider.getInstance(account)
            val localFolder = localStore.getFolder(folderId)
            localFolder.open()
            var uidMap: MutableMap<String?, String?>? = null
            val trashFolderId = account.trashFolderId
            var localTrashFolder: LocalFolder? = null
            if (!account.hasTrashFolder() || folderId == trashFolderId ||
                backend.supportsTrashFolder && !backend.isDeleteMoveToTrash
            ) {
                Timber.d("Not moving deleted messages to local Trash folder. Removing local copies.")
                if (!localOnlyMessages.isEmpty()) {
                    localFolder.destroyMessages(localOnlyMessages)
                }
                if (!syncedMessages.isEmpty()) {
                    localFolder.setFlags(syncedMessages, setOf(Flag.DELETED), true)
                }
            } else {
                Timber.d("Deleting messages in normal folder, moving")
                localTrashFolder = localStore.getFolder(trashFolderId!!)
                val messageStore = messageStoreManager.getMessageStore(account)
                val messageIds: MutableList<Long> = ArrayList()
                val messageIdToUidMapping: MutableMap<Long, String> = HashMap()
                for (message in messages) {
                    val messageId = message.databaseId
                    messageIds.add(messageId)
                    messageIdToUidMapping[messageId] = message.uid
                }
                val moveMessageIdMapping = messageStore.moveMessages(messageIds, trashFolderId)
                val destinationMapping = messageStore.getMessageServerIds(moveMessageIdMapping.values)
                uidMap = HashMap()
                for ((sourceMessageId, destinationMessageId) in moveMessageIdMapping) {
                    val sourceUid = messageIdToUidMapping[sourceMessageId]
                    val destinationUid = destinationMapping[destinationMessageId]
                    uidMap[sourceUid] = destinationUid
                }
                if (account.isMarkMessageAsReadOnDelete) {
                    val destinationMessageIds = moveMessageIdMapping.values
                    messageStore.setFlag(destinationMessageIds, Flag.SEEN, true)
                }
            }
            for (l in messagesListeners) {
                l.folderStatusChanged(account, folderId)
                if (localTrashFolder != null) {
                    l.folderStatusChanged(account, trashFolderId!!)
                }
            }
            Timber.d("Delete policy for account %s is %s", account.description, account.deletePolicy)
            val outboxFolderId = account.outboxFolderId
            if (outboxFolderId != null && folderId == outboxFolderId && supportsUpload(account)) {
                for (destinationUid in uidMap!!.values) {
                    // If the message was in the Outbox, then it has been copied to local Trash, and has
                    // to be copied to remote trash
                    val command: PendingCommand = PendingAppend.create(trashFolderId!!, destinationUid)
                    queuePendingCommand(account, command)
                }
                processPendingCommands(account)
            } else if (localFolder.isLocalOnly) {
                // Nothing to do on the remote side
            } else if (!syncedMessageUids.isEmpty()) {
                if (account.deletePolicy == DeletePolicy.ON_DELETE) {
                    if (!account.hasTrashFolder() || folderId == trashFolderId ||
                        !backend.isDeleteMoveToTrash
                    ) {
                        queueDelete(account, folderId, syncedMessageUids)
                    } else if (account.isMarkMessageAsReadOnDelete) {
                        queueMoveOrCopy(
                            account, folderId, trashFolderId!!,
                            MoveOrCopyFlavor.MOVE_AND_MARK_AS_READ, uidMap
                        )
                    } else {
                        queueMoveOrCopy(
                            account, folderId, trashFolderId!!,
                            MoveOrCopyFlavor.MOVE, uidMap
                        )
                    }
                    processPendingCommands(account)
                } else if (account.deletePolicy == DeletePolicy.MARK_AS_READ) {
                    queueSetFlag(account, localFolder.databaseId, true, Flag.SEEN, syncedMessageUids)
                    processPendingCommands(account)
                } else {
                    Timber.d("Delete policy %s prevents delete from server", account.deletePolicy)
                }
            }
            unsuppressMessages(account, messages)
        } catch (e: UnavailableStorageException) {
            Timber.i("Failed to delete message because storage is not available - trying again later.")
            throw UnavailableAccountException(e)
        } catch (me: MessagingException) {
            throw RuntimeException("Error deleting message from local store.", me)
        }
    }

    @Throws(MessagingException::class)
    fun processPendingEmptyTrash(account: Account) {
        if (!account.hasTrashFolder()) {
            return
        }
        val trashFolderId = account.trashFolderId!!
        val localStore = localStoreProvider.getInstance(account)
        val folder = localStore.getFolder(trashFolderId)
        folder.open()
        val trashFolderServerId = folder.serverId
        val backend = getBackend(account)
        backend.deleteAllMessages(trashFolderServerId)
        if (account.expungePolicy == Expunge.EXPUNGE_IMMEDIATELY && backend.supportsExpunge) {
            backend.expunge(trashFolderServerId)
        }

        // Remove all messages marked as deleted
        folder.destroyDeletedMessages()
        compact(account, null)
    }

    fun emptyTrash(account: Account, listener: MessagingListener?) {
        putBackground("emptyTrash", listener, Runnable {
            try {
                val trashFolderId = account.trashFolderId
                if (trashFolderId == null) {
                    Timber.w("No Trash folder configured. Can't empty trash.")
                    return@Runnable
                }
                val localStore = localStoreProvider.getInstance(account)
                val localFolder = localStore.getFolder(trashFolderId)
                localFolder.open()
                val isTrashLocalOnly = isTrashLocalOnly(account)
                if (isTrashLocalOnly) {
                    localFolder.clearAllMessages()
                } else {
                    localFolder.destroyLocalOnlyMessages()
                    localFolder.setFlags(setOf(Flag.DELETED), true)
                }
                for (l in messagesListeners) {
                    l.folderStatusChanged(account, trashFolderId)
                }
                if (!isTrashLocalOnly) {
                    val command: PendingCommand = PendingEmptyTrash.create()
                    queuePendingCommand(account, command)
                    processPendingCommands(account)
                }
            } catch (e: UnavailableStorageException) {
                Timber.i("Failed to empty trash because storage is not available - trying again later.")
                throw UnavailableAccountException(e)
            } catch (e: Exception) {
                Timber.e(e, "emptyTrash failed")
            }
        })
    }

    fun clearFolder(account: Account?, folderId: Long) {
        putBackground(
            "clearFolder", null
        ) { clearFolderSynchronous(account, folderId) }
    }

    @VisibleForTesting
    fun clearFolderSynchronous(account: Account?, folderId: Long) {
        try {
            val localFolder = localStoreProvider.getInstance(account!!).getFolder(folderId)
            localFolder.open()
            localFolder.clearAllMessages()
        } catch (e: UnavailableStorageException) {
            Timber.i("Failed to clear folder because storage is not available - trying again later.")
            throw UnavailableAccountException(e)
        } catch (e: Exception) {
            Timber.e(e, "clearFolder failed")
        }
    }

    /**
     * Find out whether the account type only supports a local Trash folder.
     *
     *
     *
     * Note: Currently this is only the case for POP3 accounts.
     *
     * @param account
     * The account to check.
     *
     * @return `true` if the account only has a local Trash folder that is not synchronized
     * with a folder on the server. `false` otherwise.
     */
    private fun isTrashLocalOnly(account: Account): Boolean {
        val backend = getBackend(account)
        return !backend.supportsTrashFolder
    }

    fun performPeriodicMailSync(account: Account): Boolean {
        val latch = CountDownLatch(1)
        val syncError = MutableBoolean(false)
        checkMail(account, false, false, object : SimpleMessagingListener() {
            override fun checkMailFinished(context: Context, account: Account) {
                latch.countDown()
            }

            override fun synchronizeMailboxFailed(account: Account, folderId: Long, message: String) {
                syncError.value = true
            }
        })
        Timber.v("performPeriodicMailSync(%s) about to await latch release", account.description)
        try {
            latch.await()
            Timber.v("performPeriodicMailSync(%s) got latch release", account.description)
        } catch (e: Exception) {
            Timber.e(e, "Interrupted while awaiting latch release")
        }
        val success = !syncError.value
        if (success) {
            val now = System.currentTimeMillis()
            Timber.v("Account $account successfully synced @ $now")
            account.lastSyncTime = now
            preferences.saveAccount(account)
        }
        return success
    }

    /**
     * Checks mail for one or multiple accounts. If account is null all accounts
     * are checked.
     */
    fun checkMail(
        account: Account?,
        ignoreLastCheckedTime: Boolean,
        useManualWakeLock: Boolean,
        listener: MessagingListener
    ) {
        var twakeLock: TracingWakeLock? = null
        if (useManualWakeLock) {
            val pm = TracingPowerManager.getPowerManager(context)
            twakeLock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "K9 MessagingController.checkMail")
            twakeLock.setReferenceCounted(false)
            twakeLock.acquire(K9.MANUAL_WAKE_LOCK_TIMEOUT.toLong())
        }
        val wakeLock = twakeLock
        for (l in getListeners(listener)) {
            l.checkMailStarted(context, account)
        }
        putBackground("checkMail", listener) {
            try {
                Timber.i("Starting mail check")
                val accounts: Collection<Account>
                if (account != null) {
                    accounts = ArrayList(1)
                    accounts.add(account)
                } else {
                    accounts = preferences.availableAccounts
                }
                accounts.forEach {
                    checkMailForAccount(context, it, ignoreLastCheckedTime, listener)
                }
            } catch (e: Exception) {
                Timber.e(e, "Unable to synchronize mail")
            }
            putBackground(
                "finalize sync", null
            ) {
                Timber.i("Finished mail sync")
                wakeLock?.release()
                for (l in getListeners(listener)) {
                    l.checkMailFinished(context, account)
                }
            }
        }
    }

    private fun checkMailForAccount(
        context: Context, account: Account,
        ignoreLastCheckedTime: Boolean,
        listener: MessagingListener
    ) {
        if (!account.isAvailable(context)) {
            Timber.i("Skipping synchronizing unavailable account %s", account.description)
            return
        }
        Timber.i("Synchronizing account %s", account.description)
        account.isRingNotified = false
        sendPendingMessages(account, listener)
        refreshFolderListIfStale(account)
        try {
            val aDisplayMode = account.folderDisplayMode
            val aSyncMode = account.folderSyncMode
            val localStore = localStoreProvider.getInstance(account)
            for (folder in localStore.getPersonalNamespaces(false)) {
                folder.open()
                val fDisplayClass = folder.displayClass
                val fSyncClass = folder.syncClass
                if (LocalFolder.isModeMismatch(aDisplayMode, fDisplayClass)) {
                    // Never sync a folder that isn't displayed
                    /*
                    if (K9.DEBUG) {
                        Log.v(K9.LOG_TAG, "Not syncing folder " + folder.getName() +
                              " which is in display mode " + fDisplayClass + " while account is in display mode " + aDisplayMode);
                    }
                    */
                    continue
                }
                if (LocalFolder.isModeMismatch(aSyncMode, fSyncClass)) {
                    // Do not sync folders in the wrong class
                    /*
                    if (K9.DEBUG) {
                        Log.v(K9.LOG_TAG, "Not syncing folder " + folder.getName() +
                              " which is in sync mode " + fSyncClass + " while account is in sync mode " + aSyncMode);
                    }
                    */
                    continue
                }
                synchronizeFolder(account, folder, ignoreLastCheckedTime, listener)
            }
        } catch (e: MessagingException) {
            Timber.e(e, "Unable to synchronize account %s", account.name)
        } finally {
            putBackground(
                "clear notification flag for " + account.description, null
            ) {
                Timber.v("Clearing notification flag for %s", account.description)
                account.isRingNotified = false
                if (getUnreadMessageCount(account) == 0) {
                    notificationController.clearNewMailNotifications(account)
                }
            }
        }
    }

    private fun synchronizeFolder(
        account: Account, folder: LocalFolder, ignoreLastCheckedTime: Boolean,
        listener: MessagingListener
    ) {
        putBackground("sync" + folder.serverId, null) {
            synchronizeFolderInBackground(
                account,
                folder,
                ignoreLastCheckedTime,
                listener
            )
        }
    }

    private fun synchronizeFolderInBackground(
        account: Account, folder: LocalFolder, ignoreLastCheckedTime: Boolean,
        listener: MessagingListener
    ) {
        Timber.v("Folder ${folder.serverId} was last synced @ ${folder.lastChecked}")
        if (!ignoreLastCheckedTime) {
            val lastCheckedTime = folder.lastChecked
            val now = System.currentTimeMillis()
            if (lastCheckedTime > now) {
                // The time this folder was last checked lies in the future. We better ignore this and sync now.
            } else {
                val syncInterval = account.automaticCheckIntervalMinutes * 60L * 1000L
                val nextSyncTime = lastCheckedTime + syncInterval
                if (nextSyncTime > now) {
                    Timber.v("Not syncing folder ${folder.serverId}, previously synced @ $lastCheckedTime which would be too recent for the account sync interval")
                    return
                }
            }
        }
        try {
            showFetchingMailNotificationIfNecessary(account, folder)
            try {
                synchronizeMailboxSynchronous(account, folder.databaseId, listener)
                val now = System.currentTimeMillis()
                folder.lastChecked = now
            } finally {
                clearFetchingMailNotificationIfNecessary(account)
            }
        } catch (e: Exception) {
            Timber.e(e, "Exception while processing folder %s:%s", account.description, folder.serverId)
        }
    }

    private fun showFetchingMailNotificationIfNecessary(account: Account, folder: LocalFolder) {
        if (account.isNotifySync) {
            notificationController.showFetchingMailNotification(account, folder)
        }
    }

    private fun clearFetchingMailNotificationIfNecessary(account: Account) {
        if (account.isNotifySync) {
            notificationController.clearFetchingMailNotification(account)
        }
    }

    fun compact(account: Account, ml: MessagingListener?) {
        putBackground("compact:" + account.description, ml) {
            try {
                val localStore = localStoreProvider.getInstance(account)
                val oldSize = localStore.size
                localStore.compact()
                val newSize = localStore.size
                ml?.let {
                    for (l in getListeners(it)) {
                        l.accountSizeChanged(account, oldSize, newSize)
                    }
                }
            } catch (e: UnavailableStorageException) {
                Timber.i("Failed to compact account because storage is not available - trying again later.")
                throw UnavailableAccountException(e)
            } catch (e: Exception) {
                Timber.e(e, "Failed to compact account %s", account.description)
            }
        }
    }

    fun clear(account: Account, ml: MessagingListener) {
        putBackground("clear:" + account.description, ml) {
            try {
                val localStore = localStoreProvider.getInstance(account)
                val oldSize = localStore.size
                localStore.clear()
                localStore.resetVisibleLimits(account.displayCount)
                val newSize = localStore.size
                for (l in getListeners(ml)) {
                    l.accountSizeChanged(account, oldSize, newSize)
                }
            } catch (e: UnavailableStorageException) {
                Timber.i("Failed to clear account because storage is not available - trying again later.")
                throw UnavailableAccountException(e)
            } catch (e: Exception) {
                Timber.e(e, "Failed to clear account %s", account.description)
            }
        }
    }

    fun recreate(account: Account, ml: MessagingListener) {
        putBackground("recreate:" + account.description, ml) {
            try {
                val localStore = localStoreProvider.getInstance(account)
                val oldSize = localStore.size
                localStore.recreate()
                localStore.resetVisibleLimits(account.displayCount)
                val newSize = localStore.size
                for (l in getListeners(ml)) {
                    l.accountSizeChanged(account, oldSize, newSize)
                }
            } catch (e: UnavailableStorageException) {
                Timber.i("Failed to recreate an account because storage is not available - trying again later.")
                throw UnavailableAccountException(e)
            } catch (e: Exception) {
                Timber.e(e, "Failed to recreate account %s", account.description)
            }
        }
    }

    fun deleteAccount(account: Account?) {
        notificationController.clearNewMailNotifications(account)
        memorizingMessagingListener.removeAccount(account)
    }

    /**
     * Save a draft message.
     */
    fun saveDraft(account: Account?, message: Message?, existingDraftId: Long?, plaintextSubject: String?): Long? {
        return draftOperations.saveDraft(account!!, message!!, existingDraftId, plaintextSubject)
    }

    fun getId(message: Message?): Long? {
        return if (message is LocalMessage) {
            message.databaseId
        } else {
            Timber.w("MessagingController.getId() called without a LocalMessage")
            null
        }
    }

    private class Command : Comparable<Command> {
        var runnable: Runnable? = null
        var listener: MessagingListener? = null
        var description: String? = null
        var isForegroundPriority = false
        var sequence = sequencing.getAndIncrement()
        override fun compareTo(other: Command): Int {
            return if (other.isForegroundPriority && !isForegroundPriority) {
                1
            } else if (!other.isForegroundPriority && isForegroundPriority) {
                -1
            } else {
                sequence - other.sequence
            }
        }
    }

    fun cancelNotificationsForAccount(account: Account?) {
        notificationController.clearNewMailNotifications(account)
    }

    fun cancelNotificationForMessage(account: Account?, messageReference: MessageReference?) {
        notificationController.removeNewMailNotification(account, messageReference)
    }

    fun clearCertificateErrorNotifications(account: Account?, incoming: Boolean) {
        notificationController.clearCertificateErrorNotifications(account, incoming)
    }

    fun notifyUserIfCertificateProblem(account: Account?, exception: Exception?, incoming: Boolean) {
        if (exception !is CertificateValidationException) {
            return
        }
        if (!exception.needsUserAttention()) {
            return
        }
        notificationController.showCertificateErrorNotification(account, incoming)
    }

    private fun actOnMessagesGroupedByAccountAndFolder(messages: List<MessageReference>, actor: MessageActor) {
        val accountMap = groupMessagesByAccountAndFolder(messages)
        for ((accountUuid, folderMap) in accountMap) {
            val account = preferences.getAccount(accountUuid)
            for ((folderId, messageList) in folderMap) {
                actOnMessageGroup(account, folderId, messageList, actor)
            }
        }
    }

    private fun groupMessagesByAccountAndFolder(
        messages: List<MessageReference>
    ): Map<String, MutableMap<Long, MutableList<MessageReference>>> {
        val accountMap: MutableMap<String, MutableMap<Long, MutableList<MessageReference>>> = HashMap()
        for (message in messages) {
            val accountUuid = message.accountUuid
            val folderId = message.folderId
            var folderMap = accountMap[accountUuid]
            if (folderMap == null) {
                folderMap = HashMap()
                accountMap[accountUuid] = folderMap
            }
            var messageList = folderMap[folderId]
            if (messageList == null) {
                messageList = LinkedList()
                folderMap[folderId] = messageList
            }
            messageList.add(message)
        }
        return accountMap
    }

    private fun actOnMessageGroup(
        account: Account?, folderId: Long, messageReferences: List<MessageReference>, actor: MessageActor
    ) {
        try {
            val messageFolder = localStoreProvider.getInstance(account!!).getFolder(folderId)
            val localMessages = messageFolder.getMessagesByReference(messageReferences)
            actor.act(account, messageFolder, localMessages)
        } catch (e: MessagingException) {
            Timber.e(e, "Error loading account?!")
        }
    }

    private interface MessageActor {
        fun act(account: Account, messageFolder: LocalFolder, messages: List<LocalMessage>)
    }

    internal inner class ControllerSyncListener(
        private val account: Account,
        private val listener: MessagingListener?
    ) :
        SyncListener {
        private val localStore: LocalStore = getLocalStoreOrThrow(account)
        private val previousUnreadMessageCount: Int = getUnreadMessageCount(account)
        var syncFailed = false
        override fun syncStarted(folderServerId: String) {
            val folderId = getFolderIdOrThrow(account, folderServerId)
            for (messagingListener in getListeners(listener)) {
                messagingListener.synchronizeMailboxStarted(account, folderId)
            }
        }

        override fun syncAuthenticationSuccess() {
            notificationController.clearAuthenticationErrorNotification(account, true)
        }

        override fun syncHeadersStarted(folderServerId: String) {
            for (messagingListener in getListeners(listener)) {
                messagingListener.synchronizeMailboxHeadersStarted(account, folderServerId)
            }
        }

        override fun syncHeadersProgress(folderServerId: String, completed: Int, total: Int) {
            for (messagingListener in getListeners(listener)) {
                messagingListener.synchronizeMailboxHeadersProgress(account, folderServerId, completed, total)
            }
        }

        override fun syncHeadersFinished(folderServerId: String, totalMessagesInMailbox: Int, numNewMessages: Int) {
            for (messagingListener in getListeners(listener)) {
                messagingListener.synchronizeMailboxHeadersFinished(
                    account, folderServerId, totalMessagesInMailbox,
                    numNewMessages
                )
            }
        }

        override fun syncProgress(folderServerId: String, completed: Int, total: Int) {
            val folderId = getFolderIdOrThrow(account, folderServerId)
            for (messagingListener in getListeners(listener)) {
                messagingListener.synchronizeMailboxProgress(account, folderId, completed, total)
            }
        }

        override fun syncNewMessage(folderServerId: String, messageServerId: String, isOldMessage: Boolean) {

            // Send a notification of this message
            val message = loadMessage(folderServerId, messageServerId)
            val localFolder = message.folder
            if (notificationStrategy.shouldNotifyForMessage(account, localFolder, message, isOldMessage)) {
                Timber.v("Creating notification for message %s:%s", localFolder.name, message.uid)
                // Notify with the localMessage so that we don't have to recalculate the content preview.
                notificationController.addNewMailNotification(account, message, previousUnreadMessageCount)
            }
            if (!message.isSet(Flag.SEEN)) {
                for (messagingListener in getListeners(listener)) {
                    messagingListener.synchronizeMailboxNewMessage(account, folderServerId, message)
                }
            }
        }

        override fun syncRemovedMessage(folderServerId: String, messageServerId: String) {
            for (messagingListener in getListeners(listener)) {
                messagingListener.synchronizeMailboxRemovedMessage(account, folderServerId, messageServerId)
            }
        }

        override fun syncFlagChanged(folderServerId: String, messageServerId: String) {
            var shouldBeNotifiedOf = false
            val message = loadMessage(folderServerId, messageServerId)
            if (message.isSet(Flag.DELETED) || isMessageSuppressed(message)) {
                syncRemovedMessage(folderServerId, message.uid)
            } else {
                val localFolder = message.folder
                if (notificationStrategy.shouldNotifyForMessage(account, localFolder, message, false)) {
                    shouldBeNotifiedOf = true
                }
            }

            // we're only interested in messages that need removing
            if (!shouldBeNotifiedOf) {
                val messageReference = message.makeMessageReference()
                notificationController.removeNewMailNotification(account, messageReference)
            }
        }

        override fun syncFinished(folderServerId: String) {
            val folderId = getFolderIdOrThrow(account, folderServerId)
            for (messagingListener in getListeners(listener)) {
                messagingListener.synchronizeMailboxFinished(account, folderId)
            }
        }

        override fun syncFailed(folderServerId: String, message: String, exception: Exception?) {
            syncFailed = true
            if (exception is AuthenticationFailedException) {
                handleAuthenticationFailure(account, true)
            } else {
                notifyUserIfCertificateProblem(account, exception, true)
            }
            val folderId = getFolderIdOrThrow(account, folderServerId)
            for (messagingListener in getListeners(listener)) {
                messagingListener.synchronizeMailboxFailed(account, folderId, message)
            }
        }

        override fun folderStatusChanged(folderServerId: String) {
            val folderId = getFolderIdOrThrow(account, folderServerId)
            for (messagingListener in getListeners(listener)) {
                messagingListener.folderStatusChanged(account, folderId)
            }
        }

        private fun loadMessage(folderServerId: String, messageServerId: String): LocalMessage {
            return try {
                val localFolder = localStore.getFolder(folderServerId)
                localFolder.open()
                localFolder.getMessage(messageServerId)
            } catch (e: MessagingException) {
                throw RuntimeException("Couldn't load message ($folderServerId:$messageServerId)", e)
            }
        }
    }

    enum class MoveOrCopyFlavor {
        MOVE, COPY, MOVE_AND_MARK_AS_READ
    }

    companion object {
        val SYNC_FLAGS: Set<Flag> = EnumSet.of(Flag.SEEN, Flag.FLAGGED, Flag.ANSWERED, Flag.FORWARDED)
        private const val FOLDER_LIST_STALENESS_THRESHOLD = 30 * 60 * 1000L

        @JvmStatic
        fun getInstance(): MessagingController {
            return get(MessagingController::class.java)
        }

        private fun getUidsFromMessages(messages: List<LocalMessage>): List<String> {
            val uids: MutableList<String> = ArrayList(messages.size)
            for (i in messages.indices) {
                uids.add(messages[i].uid)
            }
            return uids
        }

        private val sequencing = AtomicInteger(0)
    }

    init {
        controllerThread = Thread { runInBackground() }
        controllerThread.name = "MessagingController"
        controllerThread.start()
        addListener(memorizingMessagingListener)
        initializeControllerExtensions(controllerExtensions)
        draftOperations = DraftOperations(this, messageStoreManager, saveMessageDataCreator)
    }
}
