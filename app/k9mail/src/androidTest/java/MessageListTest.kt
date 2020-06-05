package com.fsck.k9.activity

import androidx.test.ext.junit.rules.ActivityScenarioRule
import androidx.test.ext.junit.runners.AndroidJUnit4
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class MessageListTest {

    @get:Rule
    var activityScenarioRule = ActivityScenarioRule(MessageList::class.java)

    @Test
    fun smokeTestSimplyStart() {
    }
}
