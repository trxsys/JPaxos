package lsr.service;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class provides skeletal implementation of {@link lsr.service.Service} interface to
 * simplify creating services. To create new service using this class programmer
 * needs to implement following methods:
 * <ul>
 * <li><code>executeRequest</code></li>
 * <li><code>makeSnapshot</code></li>
 * <li><code>updateToSnapshot</code></li>
 * </ul>
 * <p>
 * In most cases this methods will provide enough functionality. Creating
 * snapshots is invoked by framework. If more control for making snapshot is
 * needed then <code>Service</code> interface should be implemented.
 * <p>
 * All methods are called from the same thread, so it is not necessary to
 * synchronize them.
 *
 */
public abstract class PagerService extends AbstractService {
    // TODO: assess the impact of our modification on snapshotting functionality.
    private AtomicInteger lastExecutedSeq = new AtomicInteger(0);

    /**
     *
     * Executes one command from client on this state machine. This method will
     * be called by {@link lsr.paxos.replica.Replica}.
     *
     * @param value - value of instance to execute on this service
     * @param seqNo - sequence number of this request
     * @return generated reply which will be sent to client
     */
    protected abstract byte[] executeRequest(byte[] value, int seqNo);

    /**
     * Makes snapshot for current state of <code>Service</code>.
     * <p>
     * The same data created in this method, will be used to update state from
     * other snapshot using {@link #updateToSnapshot(byte[])} method.
     * 
     * @return the data containing current state
     */
    protected abstract byte[] makeSnapshot();

    /**
     * Updates the current state of <code>Service</code> to state from snapshot.
     * This method will be called after recovery to restore previous state, or
     * if we received new one from other replica (using catch-up).
     * 
     * @param snapshot - data used to update to new state
     */
    protected abstract void updateToSnapshot(byte[] snapshot);

    public final byte[] execute(byte[] value, int seqNo) {
        int seq;
        do {
            seq = lastExecutedSeq.get();
        } while (seq < seqNo && !lastExecutedSeq.compareAndSet(seq, seqNo));
        return executeRequest(value, seqNo);
    }

    public final void askForSnapshot(int lastNextSeq) {
        forceSnapshot(lastNextSeq);
    }

    public final void forceSnapshot(int lastNestSeq) {
        byte[] snapshot = makeSnapshot();
        fireSnapshotMade(lastExecutedSeq.get() + 1, snapshot, null);
    }

    public final void updateToSnapshot(int nextSeq, byte[] snapshot) {
        lastExecutedSeq.set(nextSeq - 1);
        updateToSnapshot(snapshot);
    }
}