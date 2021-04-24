package org.bitlap.server.raft

import com.alipay.sofa.jraft.Iterator
import com.alipay.sofa.jraft.Status
import com.alipay.sofa.jraft.core.StateMachineAdapter
import java.util.concurrent.atomic.AtomicLong

/**
 * Desc:
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/4/23
 */
class MetaStateMachine : StateMachineAdapter() {

    private val leaderTerm = AtomicLong(-1)

    override fun onApply(iter: Iterator) {
        // election only, do nothing
        while (iter.hasNext()) {
            println("On apply with term: ${iter.term} and index: ${iter.index}.")
            iter.next()
        }
    }

    override fun onLeaderStart(term: Long) {
        super.onLeaderStart(term)
        this.leaderTerm.set(term)
    }

    override fun onLeaderStop(status: Status?) {
        super.onLeaderStop(status)
        this.leaderTerm.set(-1L)
    }
}
