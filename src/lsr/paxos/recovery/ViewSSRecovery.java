package lsr.paxos.recovery;

import java.io.IOException;
import java.util.BitSet;
import java.util.logging.Logger;

import lsr.common.Dispatcher;
import lsr.common.ProcessDescriptor;
import lsr.paxos.DecideCallback;
import lsr.paxos.Paxos;
import lsr.paxos.PaxosImpl;
import lsr.paxos.RetransmittedMessage;
import lsr.paxos.Retransmitter;
import lsr.paxos.SnapshotProvider;
import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageType;
import lsr.paxos.messages.Recovery;
import lsr.paxos.messages.RecoveryAnswer;
import lsr.paxos.network.MessageHandler;
import lsr.paxos.network.Network;
import lsr.paxos.storage.SingleNumberWriter;
import lsr.paxos.storage.Storage;
import lsr.paxos.storage.SynchronousViewStorage;

public class ViewSSRecovery extends RecoveryAlgorithm implements Runnable {
    private boolean firstRun;
    private Paxos paxos;
    private final int numReplicas;
    private final int localId;
    private Storage storage;
    private Dispatcher dispatcher;
    private Retransmitter retransmitter;
    private RetransmittedMessage recoveryRetransmitter;
    private final MessageHandler recoveryRequestHandler;

    public ViewSSRecovery(SnapshotProvider snapshotProvider, DecideCallback decideCallback,
                          SingleNumberWriter writer, MessageHandler recoveryRequestHandler)
            throws IOException {
        this.recoveryRequestHandler = recoveryRequestHandler;
        numReplicas = ProcessDescriptor.getInstance().numReplicas;
        localId = ProcessDescriptor.getInstance().localId;

        storage = createStorage(writer);
        paxos = createPaxos(decideCallback, snapshotProvider, storage);
        dispatcher = paxos.getDispatcher();
    }

    public Paxos getPaxos() {
        return paxos;
    }

    public void start() {
        dispatcher.dispatch(this);
    }

    public void run() {
        // do not execute recovery mechanism on first run
        if (firstRun) {
            onRecoveryFinished();
            return;
        }

        retransmitter = new Retransmitter(paxos.getNetwork(), numReplicas, dispatcher);
        logger.info("Sending recovery message");
        Network.addMessageListener(MessageType.RecoveryAnswer, new RecoveryAnswerListener());
        recoveryRetransmitter = retransmitter.startTransmitting(new Recovery(storage.getView(), -1));
    }

    protected Paxos createPaxos(DecideCallback decideCallback, SnapshotProvider snapshotProvider,
                                Storage storage) throws IOException {
        return new PaxosImpl(decideCallback, snapshotProvider, storage);
    }

    private Storage createStorage(SingleNumberWriter writer) {
        Storage storage = new SynchronousViewStorage(writer);
        firstRun = storage.getView() == 0;
        if (storage.getView() % numReplicas == localId)
            storage.setView(storage.getView() + 1);
        return storage;
    }

    // Get all instances before <code>nextId</code>
    private void startCatchup(final int nextId) {
        new RecoveryCatchUp(paxos.getCatchup(), storage).recover(nextId, new Runnable() {
            public void run() {
                onRecoveryFinished();
            }
        });
    }

    private void onRecoveryFinished() {
        fireRecoveryListener();
        Network.addMessageListener(MessageType.Recovery, recoveryRequestHandler);
    }

    private class RecoveryAnswerListener implements MessageHandler {
        private BitSet received;
        private RecoveryAnswer answerFromLeader = null;

        public RecoveryAnswerListener() {
            received = new BitSet(numReplicas);
        }

        public void onMessageReceived(Message msg, final int sender) {
            assert msg.getType() == MessageType.RecoveryAnswer;
            final RecoveryAnswer recoveryAnswer = (RecoveryAnswer) msg;

            // drop messages from lower views
            if (recoveryAnswer.getView() < storage.getView())
                return;

            logger.info("Got a recovery answer " + recoveryAnswer +
                        (recoveryAnswer.getView() % numReplicas == sender ? " from leader" : ""));

            dispatcher.dispatch(new Runnable() {
                public void run() {
                    recoveryRetransmitter.stop(sender);
                    received.set(sender);

                    // update view
                    if (storage.getView() < recoveryAnswer.getView()) {
                        storage.setView(recoveryAnswer.getView());
                        answerFromLeader = null;
                    }

                    if (storage.getView() % numReplicas == sender) {
                        answerFromLeader = recoveryAnswer;
                    }

                    if (received.cardinality() > numReplicas / 2) {
                        onCardinality();
                    }
                }
            });
        }

        private void onCardinality() {
            recoveryRetransmitter.stop();
            recoveryRetransmitter = null;

            if (answerFromLeader == null) {
                Recovery recovery = new Recovery(storage.getView(), -1);
                recoveryRetransmitter = retransmitter.startTransmitting(recovery);
            } else {
                startCatchup((int) answerFromLeader.getNextId());
                Network.removeMessageListener(MessageType.RecoveryAnswer, this);
            }
        }

        public void onMessageSent(Message message, BitSet destinations) {
        }
    }

    private static final Logger logger = Logger.getLogger(ViewSSRecovery.class.getCanonicalName());
}
