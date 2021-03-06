package lsr.paxos.replica;

import lsr.common.*;
import lsr.paxos.Batcher;
import lsr.paxos.Paxos;
import lsr.paxos.Snapshot;
import lsr.paxos.SnapshotProvider;
import lsr.paxos.recovery.*;
import lsr.paxos.replica.ClientBatchStore.ClientBatchInfo;
import lsr.paxos.statistics.PerformanceLogger;
import lsr.paxos.statistics.ReplicaStats;
import lsr.paxos.storage.ConsensusInstance;
import lsr.paxos.storage.ConsensusInstance.LogEntryState;
import lsr.paxos.storage.SingleNumberWriter;
import lsr.paxos.storage.Storage;
import lsr.service.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages replication of a service. Receives requests from the client, orders
 * them using Paxos, executes the ordered requests and sends the reply back to
 * the client.
 * <p>
 * Example of usage:
 * <p>
 * <blockquote>
 * 
 * <pre>
 * public static void main(String[] args) throws IOException {
 *  int localId = Integer.parseInt(args[0]);
 *  Replica replica = new Replica(localId, new MapService());
 *  replica.start();
 * }
 * </pre>
 * 
 * </blockquote>
 */
public class Replica {
    /**
     * Represents different crash models.
     */
    public enum CrashModel {
        /**
         * Synchronous writes to disk are made on critical path of paxos
         * execution. Catastrophic failures will be handled correctly. It is
         * slower than other algorithms.
         */
        FullStableStorage,
        CrashStop,
        EpochSS,
        ViewSS
    }

    private String logPath;

    private Paxos paxos;
    private final ServiceProxy serviceProxy;
    private volatile NioClientManager clientManager;

    private static final boolean LOG_DECISIONS = false;
    /** Used to log all decisions. */
    private final PerformanceLogger decisionsLog;

    /** Next request to be executed. */
    private int executeUB = 0;

    private ClientRequestManager requestManager;

    // TODO: JK check if this map is cleared where possible
    /** caches responses for clients */
    private final Map<Integer, List<Reply>> executedDifference =
            new HashMap<Integer, List<Reply>>();

    /**
     * For each client, keeps the sequence id of the last request executed from
     * the client.
     * 
     * TODO: the executedRequests map grows and is NEVER cleared!
     * 
     * For theoretical correctness, it must stay so. In practical approach, give
     * me unbounded storage, limit the overall client count or simply let eat
     * some stale client requests.
     * 
     * Bad solution keeping correctness: record time stamp from client, his
     * request will only be valid for 5 minutes, after that time - go away. If
     * client resends it after 5 minutes, we ignore request. If client changes
     * the time stamp, it's already a new request, right? Client with broken
     * clocks will have bad luck.
     * 
     * This is accessed by the Selector threads, so it must be thread-safe
     */
    private final Map<Long, Reply> executedRequests =
            new ConcurrentHashMap<Long, Reply>(8192, (float) 0.75, 8);  



    private final HashMap<Long, Reply> previousSnapshotExecutedRequests = new HashMap<Long, Reply>();

    private final SingleThreadDispatcher dispatcher;
    private final ProcessDescriptor descriptor;

    private final SnapshotListener2 innerSnapshotListener2;
    private final SnapshotProvider innerSnapshotProvider;

    private final Configuration config;

    private ArrayList<Reply> cache;


    /**
     * Executor service for parallel batch execution
     */
    private final boolean parallelExecution;
    private ExecutorService execService;
    private Semaphore instanceRequestsFinished;


    /**
     * Initializes new instance of <code>Replica</code> class.
     * <p>
     * This constructor doesn't start the replica and Paxos protocol. In order
     * to run it the {@link #start()} method should be called.
     * 
     * @param config - the configuration of the replica
     * @param localId - the id of replica to create
     * @param service - the state machine to execute request on
     * @throws IOException if an I/O error occurs
     */
    public Replica(Configuration config, int localId, Service service) throws IOException {
        this.innerSnapshotListener2 = new InnerSnapshotListener2();
        this.innerSnapshotProvider = new InnerSnapshotProvider();
        this.dispatcher = new SingleThreadDispatcher("Replica");
        this.config = config;

        ProcessDescriptor.initialize(config, localId);
        descriptor = ProcessDescriptor.getInstance();

        logPath = descriptor.logPath + '/' + localId;

        // Open the log file with the decisions
        if (LOG_DECISIONS) {
            decisionsLog = PerformanceLogger.getLogger("decisions-" + localId);
        } else {
            decisionsLog = null;
        }

        serviceProxy = new ServiceProxy(service, executedDifference, dispatcher);
        serviceProxy.addSnapshotListener(innerSnapshotListener2);

        cache = new ArrayList<Reply>(2048);
        executedDifference.put(executeUB, cache);

        parallelExecution = config.getBooleanProperty("parallel.batch", false);
        if (parallelExecution) {
            execService = Executors.newFixedThreadPool(config.getIntProperty("parallel.batch.workers", 4));
            instanceRequestsFinished = new Semaphore(1);
        }
    }

    /**
     * Starts the replica.
     * <p>
     * First the recovery phase is started and after that the replica joins the
     * Paxos protocol and starts the client manager and the underlying service.
     * 
     * @throws IOException if some I/O error occurs
     */
    public void start() throws IOException {
        logger.info("Recovery phase started.");

        dispatcher.start();
        RecoveryAlgorithm recovery = createRecoveryAlgorithm(descriptor.crashModel);
        paxos = recovery.getPaxos();

        // TODO TZ - the dispatcher and network has to be started before
        // recovery phase.
        // FIXME: NS - For CrashStop this is not needed. For the other recovery algorithms, 
        // this must be fixed. Starting the network before the Paxos module can cause problems, 
        // if some protocol message is received before Paxos has started. These messages will
        // be ignored, which will 
        //        paxos.getDispatcher().start();
        //        paxos.getNetwork().start();
        //        paxos.getCatchup().start();

        recovery.addRecoveryListener(new InnerRecoveryListener());
        recovery.start();
    }

    private RecoveryAlgorithm createRecoveryAlgorithm(CrashModel crashModel) throws IOException {
        switch (crashModel) {
            case CrashStop:
                return new CrashStopRecovery(innerSnapshotProvider);
            case FullStableStorage:
                return new FullSSRecovery(innerSnapshotProvider, logPath);
            case EpochSS:
                return new EpochSSRecovery(innerSnapshotProvider, logPath);
            case ViewSS:
                return new ViewSSRecovery(innerSnapshotProvider,
                        new SingleNumberWriter(logPath, "sync.view"));
            default:
                throw new RuntimeException("Unknown crash model: " + crashModel);
        }
    }

    public void forceExit() {
        dispatcher.shutdownNow();
    }

    /**
     * Sets the path to directory where all logs will be saved.
     * 
     * @param path to directory where logs will be saved
     */
    public void setLogPath(String path) {
        logPath = path;
    }

    /**
     * Gets the path to directory where all logs will be saved.
     * 
     * @return path
     */
    public String getLogPath() {
        return logPath;
    }

    public Configuration getConfiguration() {
        return config;
    }


    public void executeNopInstance(final int nextInstance) {
        logger.warning("Executing a nop request. Instance: " + executeUB);
        dispatcher.execute(new Runnable() {
            @Override
            public void run() {
                serviceProxy.executeNop();
            }
        });
    }
    
    public void executeClientBatch(final int instance, final ClientBatchInfo bInfo) {
        dispatcher.execute(new Runnable() {
            @Override
            public void run() {
                innerExecuteClientBatch(instance, bInfo);
            }
        });
    }

    public void executeUnorderedClientRequest(final ClientRequest cRequest) {
        execService.execute(new Runnable() {
            @Override
            public void run() {
                innerExecuteUnorderedClientRequest(cRequest);
            }
        });
    }

    /** 
     * Called by the RequestManager when it has the ClientRequest that should be
     * executed next. 
     * 
     * @param instance
     * @param bInfo
     */
    private void innerExecuteClientBatch(final int instance, final ClientBatchInfo bInfo) {
        assert dispatcher.amIInDispatcher() : "Wrong thread: " + Thread.currentThread().getName();

        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Executing batch " + bInfo + ", instance number " + instance) ;
        }        

        final ClientRequest[] batch = bInfo.batch;
        final int batchLen = batch.length;

        for (int i = 0; i < batchLen; i++) {
            final ClientRequest cRequest = batch[i];
            final int reqBatchPos = i;
            if (innerPrepareClientRequest(instance, bInfo, cRequest)) {
                if (parallelExecution) {
                    logger.info("[ParallelBatch] Executing client request in parallel batchPos="+reqBatchPos);
                    execService.execute(new Runnable() {
                        @Override
                        public void run() { innerExecuteClientRequest(instance, bInfo, cRequest);
                        }
                    });
                }
                else {
                    innerExecuteClientRequest(instance, bInfo, cRequest);
                }
            }
        }
    }

    private final boolean innerPrepareClientRequest(final int instance, final ClientBatchInfo bInfo, ClientRequest cRequest) {
        RequestId rID = cRequest.getRequestId();
        Reply lastReply = executedRequests.get(rID.getClientId());
        if (lastReply != null) {
            int lastSequenceNumberFromClient = lastReply.getRequestId().getSeqNumber();
            // Do not execute the same request several times.
            if (rID.getSeqNumber() <= lastSequenceNumberFromClient) {
                logger.warning("Request ordered multiple times. " +
                        instance + ", batch: " + bInfo.bid + ", " + cRequest + ", lastSequenceNumberFromClient: " + lastSequenceNumberFromClient);
                // Send the cached reply back to the client
                if (rID.getSeqNumber() == lastSequenceNumberFromClient) {
                    requestManager.onRequestExecuted(cRequest, lastReply);
                }
                return false;
            }
        }
        if (parallelExecution) instanceRequestsFinished.release();
        serviceProxy.prepare(cRequest);
        return true;
    }

    private void innerExecuteClientRequest(int instance, ClientBatchInfo bInfo, ClientRequest cRequest) {
        RequestId rID = cRequest.getRequestId();

        // Here the replica thread is given to Service.
        byte[] result = serviceProxy.execute(cRequest);
        // Statistics. Count how many requests are in this instance
        requestsInInstance++;

        Reply reply = new Reply(cRequest.getRequestId(), result);

        if (LOG_DECISIONS) {
            assert decisionsLog != null : "Decision log cannot be null";
            decisionsLog.logln(instance + ":" + cRequest.getRequestId());
        }

        // add request to executed history
        cache.add(reply);

        executedRequests.put(rID.getClientId(), reply);

        if (parallelExecution) {
            try {
                instanceRequestsFinished.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Can this ever be null?
        assert requestManager != null : "Request manager should not be null";
        requestManager.onRequestExecuted(cRequest, reply);
    }

    /**
     * Called directly from ClientRequestManager bypassing Paxos service
     * @param cRequest - client request
     */
    public void innerExecuteUnorderedClientRequest(ClientRequest cRequest) {
        RequestId rID = cRequest.getRequestId();
        Reply lastReply = executedRequests.get(rID.getClientId());
        if (lastReply != null) {
            int lastSequenceNumberFromClient = lastReply.getRequestId().getSeqNumber();

            // Do not execute the same request several times.
            if (rID.getSeqNumber() <= lastSequenceNumberFromClient) {
                logger.warning("Unordered request ordered multiple times. " +
                        cRequest + ", lastSequenceNumberFromClient: " + lastSequenceNumberFromClient);

                // Send the cached reply back to the client
                if (rID.getSeqNumber() == lastSequenceNumberFromClient) {
                    requestManager.onRequestExecuted(cRequest, lastReply);
                }
                return;
            }
        }

        // Here the replica thread is given to Service.
        byte[] result = serviceProxy.execute(cRequest);

        Reply reply = new Reply(cRequest.getRequestId(), result);

        // add request to executed history
        cache.add(reply);

        executedRequests.put(rID.getClientId(), reply);

        // Can this ever be null?
        assert requestManager != null : "Request manager should not be null";
        requestManager.onRequestExecuted(cRequest, reply);
    }

    // Statistics. Used to count how many requests are in a given instance.
    private int requestsInInstance = 0;

    /** Called by RequestManager when it finishes executing a batch */
    public void instanceExecuted(final int instance) {
        dispatcher.execute(new Runnable() {
            @Override
            public void run() {
                innerInstanceExecuted(instance);
            }
        });
    }

    void innerInstanceExecuted(final int instance) {
        if (parallelExecution) {
            try {
                instanceRequestsFinished.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            instanceRequestsFinished.release();
        }

        if (logger.isLoggable(Level.INFO)) {
            logger.info("Instance finished: " + instance);
        }
        serviceProxy.instanceExecuted(instance);
        cache = new ArrayList<Reply>(2048);
        executedDifference.put(instance+1, cache);
        
        executeUB=instance+1;

        // The ReplicaStats must be updated only from the Protocol thread
        final int fReqCount = requestsInInstance;
        paxos.getDispatcher().submit(new Runnable() {
            @Override
            public void run() {
                ReplicaStats.getInstance().setRequestsInInstance(instance, fReqCount);
            }}  );
        requestsInInstance = 0;
    }
    
    /**
     * Listener called after recovery algorithm is finished and paxos can be
     * started.
     */
    private class InnerRecoveryListener implements RecoveryListener {
        public void recoveryFinished() {
            recoverReplica();

            dispatcher.execute(new Runnable() {
                public void run() {
                    serviceProxy.recoveryFinished();
                }
            });

            IdGenerator idGenerator = createIdGenerator();
            int clientPort = descriptor.getLocalProcess().getClientPort();
            requestManager = new ClientRequestManager(Replica.this, paxos, executedRequests);
            paxos.setClientRequestManager(requestManager);
            paxos.setDecideCallback(requestManager.getClientBatchManager());

            try {
                clientManager = new NioClientManager(clientPort, requestManager, idGenerator);
                clientManager.start();
            } catch (IOException e) {
                throw new RuntimeException("Could not prepare the socket for clients! Aborting.");
            }

            logger.info("Recovery phase finished. Starting paxos protocol.");
            paxos.startPaxos();
        }

        private void recoverReplica() {
            Storage storage = paxos.getStorage();

            // we need a read-write copy of the map
            SortedMap<Integer, ConsensusInstance> instances =
                    new TreeMap<Integer, ConsensusInstance>();
            instances.putAll(storage.getLog().getInstanceMap());

            // We take the snapshot
            Snapshot snapshot = storage.getLastSnapshot();
            if (snapshot != null) {
                innerSnapshotProvider.handleSnapshot(snapshot);
                instances = instances.tailMap(snapshot.getNextInstanceId());
            }

            for (ConsensusInstance instance : instances.values()) {
                if (instance.getState() == LogEntryState.DECIDED) {
                    Deque<ClientBatch> requests = Batcher.unpack(instance.getValue());
                    requestManager.getClientBatchManager().onRequestOrdered(instance.getId(), requests);
                }
            }
            storage.updateFirstUncommitted();
        }

        private IdGenerator createIdGenerator() {
            String generatorName = ProcessDescriptor.getInstance().clientIDGenerator;
            if (generatorName.equals("TimeBased")) {
                return new TimeBasedIdGenerator(descriptor.localId, descriptor.numReplicas);
            }
            if (generatorName.equals("Simple")) {
                return new SimpleIdGenerator(descriptor.localId, descriptor.numReplicas);
            }
            throw new RuntimeException("Unknown id generator: " + generatorName +
                    ". Valid options: {TimeBased, Simple}");
        }
    }

    private class InnerSnapshotListener2 implements SnapshotListener2 {
        public void onSnapshotMade(final Snapshot snapshot) {
            dispatcher.checkInDispatcher();

            if (snapshot.getValue() == null) {
                throw new RuntimeException("Received a null snapshot!");
            }

            // add header to snapshot
            Map<Long, Reply> requestHistory = 
                    new HashMap<Long, Reply>(previousSnapshotExecutedRequests);

            // Get previous snapshot next instance id
            int prevSnapshotNextInstId;
            Snapshot lastSnapshot = paxos.getStorage().getLastSnapshot();
            if (lastSnapshot != null) {
                prevSnapshotNextInstId = lastSnapshot.getNextInstanceId();
            } else {
                prevSnapshotNextInstId = 0;
            }

            // update map to state in moment of snapshot
            for (int i = prevSnapshotNextInstId; i < snapshot.getNextInstanceId(); ++i) {
                List<Reply> ides = executedDifference.remove(i);

                // this is null only when NoOp
                if (ides == null) {
                    continue;
                }

                for (Reply reply : ides) {
                    requestHistory.put(reply.getRequestId().getClientId(), reply);
                }
            }

            snapshot.setLastReplyForClient(requestHistory);

            previousSnapshotExecutedRequests.clear();
            previousSnapshotExecutedRequests.putAll(requestHistory);

            paxos.onSnapshotMade(snapshot);
        }
    }

    private class InnerSnapshotProvider implements SnapshotProvider {
        public void handleSnapshot(final Snapshot snapshot) {            
            logger.info("New snapshot received");
            dispatcher.execute(new Runnable() {
                public void run() {
                    handleSnapshotInternal(snapshot);
                }
            });
        }

        public void askForSnapshot() {
            dispatcher.execute(new Runnable() {
                public void run() {
                    serviceProxy.askForSnapshot();
                }
            });
        }

        public void forceSnapshot() {
            dispatcher.execute(new Runnable() {
                public void run() {
                    serviceProxy.forceSnapshot();
                }
            });
        }

        /**
         * Restoring state from a snapshot
         * 
         * @param snapshot
         */
        private void handleSnapshotInternal(Snapshot snapshot) {
            assert dispatcher.amIInDispatcher();
            assert snapshot != null : "Snapshot is null";
            
            // TODO: Obsolete code

//            if (snapshot.getNextInstanceId() < executeUB) {
//                logger.warning("Received snapshot is older than current state." +
//                        snapshot.getNextInstanceId() + ", executeUB: " + executeUB);
//                return;
//            }
//
//            logger.info("Updating machine state from snapshot." + snapshot);
//            serviceProxy.updateToSnapshot(snapshot);
//            synchronized (decidedWaitingExecution) {
//                if (!decidedWaitingExecution.isEmpty()) {
//                    if (decidedWaitingExecution.lastKey() < snapshot.getNextInstanceId()) {
//                        decidedWaitingExecution.clear();
//                    } else {
//                        while (decidedWaitingExecution.firstKey() < snapshot.getNextInstanceId()) {
//                            decidedWaitingExecution.pollFirstEntry();
//                        }
//                    }
//                }
//            }
//
//            executedRequests.clear();
//            executedDifference.clear();
//            executedRequests.putAll(snapshot.getLastReplyForClient());
//            previousSnapshotExecutedRequests.clear();
//            previousSnapshotExecutedRequests.putAll(snapshot.getLastReplyForClient());
//            executeUB = snapshot.getNextInstanceId();
//
//            final Object snapshotLock = new Object();
//
//            synchronized (snapshotLock) {
//                AfterCatchupSnapshotEvent event = new AfterCatchupSnapshotEvent(snapshot,
//                        paxos.getStorage(), snapshotLock);
//                paxos.getDispatcher().submit(event);
//
//                try {
//                    while (!event.isFinished()) {
//                        snapshotLock.wait();
//                    }
//                } catch (InterruptedException e) {
//                    Thread.currentThread().interrupt();
//                }
//            }
        }
    }

    public SingleThreadDispatcher getReplicaDispatcher() {
        return dispatcher;
    }

    private final static Logger logger = Logger.getLogger(Replica.class.getCanonicalName());



}
