package com.fsck.k9.service

import android.app.Service
import android.content.Context
import com.fsck.k9.Preferences.Companion.getPreferences
import com.fsck.k9.DI.get
import com.fsck.k9.K9.setDatabasesUpToDate
import androidx.localbroadcastmanager.content.LocalBroadcastManager
import com.fsck.k9.power.TracingPowerManager.TracingWakeLock
import android.content.Intent
import android.os.IBinder
import android.os.PowerManager
import timber.log.Timber
import com.fsck.k9.power.TracingPowerManager
import com.fsck.k9.mailstore.LocalStoreProvider
import com.fsck.k9.mailstore.UnavailableStorageException
import java.lang.Exception
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Service used to upgrade the accounts' databases and/or track the progress of the upgrade.
 *
 *
 *
 * See `UpgradeDatabases` for a detailed explanation of the database upgrade process.
 *
 */
class DatabaseUpgradeService : Service() {
    /**
     * Stores whether or not this service was already running when
     * [.onStartCommand] is executed.
     */
    private val running = AtomicBoolean(false)
    private var localBroadcastManager: LocalBroadcastManager? = null
    private var wakeLock: TracingWakeLock? = null
    override fun onBind(intent: Intent): IBinder? {
        // unused
        return null
    }

    override fun onCreate() {
        localBroadcastManager = LocalBroadcastManager.getInstance(this)
    }

    override fun onStartCommand(intent: Intent, flags: Int, startId: Int): Int {
        val success = running.compareAndSet(false, true)
        if (success) {
            // The service wasn't running yet.
            Timber.i("DatabaseUpgradeService started")
            acquireWakelock()
            startUpgradeInBackground()
        }
        return START_STICKY
    }

    /**
     * Acquire a partial wake lock so the CPU won't go to sleep when the screen is turned off.
     */
    private fun acquireWakelock() {
        val pm = TracingPowerManager.getPowerManager(this)
        wakeLock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, WAKELOCK_TAG)
        wakeLock?.setReferenceCounted(false)
        wakeLock?.acquire(WAKELOCK_TIMEOUT)
    }

    /**
     * Release the wake lock.
     */
    private fun releaseWakelock() {
        wakeLock!!.release()
    }

    /**
     * Stop this service.
     */
    private fun stopService() {
        stopSelf()
        Timber.i("DatabaseUpgradeService stopped")
        releaseWakelock()
        running.set(false)
    }

    /**
     * Start a background thread for upgrading the databases.
     */
    private fun startUpgradeInBackground() {
        object : Thread("DatabaseUpgradeService") {
            override fun run() {
                upgradeDatabases()
                stopService()
            }
        }.start()
    }

    /**
     * Upgrade the accounts' databases.
     */
    private fun upgradeDatabases() {
        val preferences = getPreferences(this)
        val accounts = preferences.accounts
        accounts.forEachIndexed { index, account ->
            sendProgressBroadcast(account.uuid, index, accounts.size)
            try {
                // Account.getLocalStore() is blocking and will upgrade the database if necessary
                get(LocalStoreProvider::class.java).getInstance(account)
            } catch (e: UnavailableStorageException) {
                Timber.e("Database unavailable")
            } catch (e: Exception) {
                Timber.e(e, "Error while upgrading database")
            }
        }
        setDatabasesUpToDate(true)
        sendUpgradeCompleteBroadcast()
    }

    private fun sendProgressBroadcast(accountUuid: String?, progress: Int, progressEnd: Int) {
        val intent = Intent()
        intent.action = ACTION_UPGRADE_PROGRESS
        intent.putExtra(EXTRA_ACCOUNT_UUID, accountUuid)
        intent.putExtra(EXTRA_PROGRESS, progress)
        intent.putExtra(EXTRA_PROGRESS_END, progressEnd)
        localBroadcastManager!!.sendBroadcast(intent)
    }

    private fun sendUpgradeCompleteBroadcast() {
        val intent = Intent()
        intent.action = ACTION_UPGRADE_COMPLETE
        localBroadcastManager!!.sendBroadcast(intent)
    }

    companion object {
        /**
         * Broadcast intent reporting the current progress of the database upgrade.
         *
         * Extras:
         *
         *  * [.EXTRA_ACCOUNT_UUID]
         *  * [.EXTRA_PROGRESS]
         *  * [.EXTRA_PROGRESS_END]
         *
         */
        const val ACTION_UPGRADE_PROGRESS = "DatabaseUpgradeService.upgradeProgress"

        /**
         * Broadcast intent sent when the upgrade has been completed.
         */
        const val ACTION_UPGRADE_COMPLETE = "DatabaseUpgradeService.upgradeComplete"

        /**
         * UUID of the account whose database is currently being upgraded.
         */
        const val EXTRA_ACCOUNT_UUID = "account_uuid"

        /**
         * The current progress.
         *
         * Integer from `0` (inclusive) to the value in [.EXTRA_PROGRESS_END]
         * (exclusive).
         */
        const val EXTRA_PROGRESS = "progress"

        /**
         * Number of items that will be upgraded.
         *
         * Currently this is the number of accounts.
         */
        const val EXTRA_PROGRESS_END = "progress_end"

        /**
         * Action used to start this service.
         */
        private const val ACTION_START_SERVICE = "com.fsck.k9.service.DatabaseUpgradeService.startService"
        private const val WAKELOCK_TAG = "DatabaseUpgradeService"
        private const val WAKELOCK_TIMEOUT = (10 * 60 * 1000).toLong() // 10 minutes

        /**
         * Start [DatabaseUpgradeService].
         *
         * @param context
         * The [Context] used to start this service.
         */
        @JvmStatic
        fun startService(context: Context) {
            val i = Intent()
            i.setClass(context, DatabaseUpgradeService::class.java)
            i.action = ACTION_START_SERVICE
            context.startService(i)
        }
    }
}
