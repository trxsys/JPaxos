package lsr.paxos.test;

import lsr.service.AbstractService;

import java.util.Random;
import java.util.logging.Logger;

public class EchoService extends AbstractService {
    private byte[] last = new byte[0];
    private final Random random;

    public EchoService() {
        super();
        random = new Random(System.currentTimeMillis() + this.hashCode());
    }

    public byte[] execute(byte[] value, int seqNo, int batchOrderId) {
        Logger.getLogger(this.getClass().getCanonicalName()).info(
                "<Service> Executed request no." + seqNo);
        if (random.nextInt(10) == 0) {
            assert (last != null);
            fireSnapshotMade(seqNo + 1, new byte[] {1}, value);
            Logger.getLogger(this.getClass().getCanonicalName()).info("Made snapshot");
        }
        last = value;
        return value;
    }

    public void askForSnapshot(int lastSnapshotInstance) {
        // ignore
    }

    public void forceSnapshot(int lastSnapshotInstance) {
        // ignore
    }

    public void updateToSnapshot(int instanceId, byte[] snapshot) {
        // ignore
    }
}
