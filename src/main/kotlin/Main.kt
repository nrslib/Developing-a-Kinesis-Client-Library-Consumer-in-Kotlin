import com.squareup.wire.schema.internal.optionValueToInt
import org.slf4j.LoggerFactory
import org.apache.commons.lang3.ObjectUtils
import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.RandomUtils
import org.slf4j.Logger
import org.slf4j.MDC
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest
import software.amazon.kinesis.common.ConfigsBuilder
import software.amazon.kinesis.common.KinesisClientUtil
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.exceptions.InvalidStateException
import software.amazon.kinesis.exceptions.ShutdownException
import software.amazon.kinesis.lifecycle.events.*
import software.amazon.kinesis.processor.ShardRecordProcessor
import software.amazon.kinesis.processor.ShardRecordProcessorFactory
import software.amazon.kinesis.retrieval.polling.PollingConfig
import java.util.*
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.system.exitProcess

val log = LoggerFactory.getLogger(SampleSingle::class.java)

fun main(args: Array<String>) {
    if (args.size < 0) {
        log.error("At a minimum, the stream name is required as the first argument. The Region may be specified as the second argument.")
        exitProcess(1)
    }

    val streamName = args[0]
    val region = if (args.size > 1) args[1] else null

    SampleSingle(streamName, region).run()
}

class SampleSingle(private val streamName: String, rawRegion: String?) {
    private val region = Region.of(ObjectUtils.firstNonNull(rawRegion, "us-east-2"))
    private val kinesisClient = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().region(region))

    fun run() {
        /**
         * Sends dummy data to Kinesis. Not relevant to consuming the data with the KCL
         */
        val producerExecutor = Executors.newSingleThreadScheduledExecutor()
        val producerFuture = producerExecutor.scheduleAtFixedRate(this::publishRecord, 10, 1, TimeUnit.SECONDS)

        /**
         * Sets up configuration for the KCL, including DynamoDB and CloudWatch dependencies. The final argument, a
         * ShardRecordProcessorFactory, is where the logic for record processing lives, and is located in a private
         * class below.
         */
        val dynamoClient = DynamoDbAsyncClient.builder().region(region).build()
        val cloudWatchClient = CloudWatchAsyncClient.builder().region(region).build()
        val configBuilder = ConfigsBuilder(
            streamName,
            streamName,
            kinesisClient,
            dynamoClient,
            cloudWatchClient,
            UUID.randomUUID().toString(),
            SampleRecordProcessorFactory()
        )

        /**
         * The Scheduler (also called Worker in earlier versions of the KCL) is the entry point to the KCL. This
         * instance is configured with defaults provided by the ConfigsBuilder.
         */
        val scheduler = Scheduler(
            configBuilder.checkpointConfig(),
            configBuilder.coordinatorConfig(),
            configBuilder.leaseManagementConfig(),
            configBuilder.lifecycleConfig(),
            configBuilder.metricsConfig(),
            configBuilder.processorConfig(),
            configBuilder.retrievalConfig().retrievalSpecificConfig(PollingConfig(streamName, kinesisClient))
        )

        /**
         * Kickoff the Scheduler. Record processing of the stream of dummy data will continue indefinitely
         * until an exit is triggered.
         */
        val schedulerThread = Thread(scheduler)
        schedulerThread.isDaemon = true
        schedulerThread.start()

        /**
         * Allows termination of app by pressing Enter.
         */
        println("Press enter to shutdown.")
        readLine()

        /**
         * Stops sending dummy data.
         */
        log.info("Canceling produce and shutting down executor.")
        producerFuture.cancel(true)
        producerExecutor.shutdownNow()

        /**
         * Stops consuming data. Finishes processing the current batch of data already received from Kinesis
         * before shutting down.
         */
        val gracefulShutdownFuture = scheduler.startGracefulShutdown()
        log.info("Waiting up to 20 seconds for shutdown to complete.")
        try {
            gracefulShutdownFuture.get(20, TimeUnit.SECONDS)
        } catch (e: InterruptedException) {
            log.info("Interrupted while waiting for graceful shutdown. Continuing.")
        } catch (e: ExecutionException) {
            log.error("Exception while executing graceful shutdown.", e)
        } catch (e: TimeoutException) {
            log.error("Timeout while waiting for shutdown. Scheduler may not have exited.")
        }
        log.info("Completed, shutting down now.")
    }

    private fun publishRecord() {
        val request = PutRecordRequest.builder()
            .partitionKey(RandomStringUtils.randomAlphabetic(5, 20))
            .streamName(streamName)
            .data(SdkBytes.fromByteArray(RandomUtils.nextBytes(10)))
            .build()

        try {
            kinesisClient.putRecord(request).get()
        } catch (e: InterruptedException) {
            log.info("Interrupted, assuming shutdown.")
        }catch (e: ExecutionException) {
            log.error("Exception while sending data to Kinesis. Will try again next cycle.", e)
        }
    }

    class SampleRecordProcessorFactory : ShardRecordProcessorFactory {
        override fun shardRecordProcessor(): ShardRecordProcessor = SampleRecordProcessor()
    }

    class SampleRecordProcessor : ShardRecordProcessor {
        companion object {
            const val SHARD_ID_MDC_KEY = "ShardId"
            val log: Logger = LoggerFactory.getLogger(SampleRecordProcessor::class.java)
        }

        private lateinit var shardId: String

        override fun initialize(initializationInput: InitializationInput) {
            shardId = initializationInput.shardId()
            MDC.put(SHARD_ID_MDC_KEY, shardId)
            try {
                log.info("Initializing @ Sequence: {}", initializationInput.extendedSequenceNumber())
            } finally {
                MDC.remove(SHARD_ID_MDC_KEY)
            }
        }

        override fun processRecords(processRecordsInput: ProcessRecordsInput) {
            MDC.put(SHARD_ID_MDC_KEY, shardId)
            try {
                log.info("Processing ${processRecordsInput.records().size}")
                processRecordsInput.records()
                    .forEach { log.info("Processing record pk: {} -- Seq: {}", it.partitionKey(), it.sequenceNumber()) }
            } catch (t: Throwable) {
                log.error("Caught throwable while processing records. Aborting.")
                Runtime.getRuntime().halt(1)
            } finally {
                MDC.remove(SHARD_ID_MDC_KEY)
            }
        }

        override fun leaseLost(leaseLostInput: LeaseLostInput) {
            MDC.put(SHARD_ID_MDC_KEY, shardId)
            try {
                log.info("Lost lease, so terminating.")
            } finally {
                MDC.remove(SHARD_ID_MDC_KEY)
            }
        }

        override fun shardEnded(shardEndedInput: ShardEndedInput) {
            MDC.put(SHARD_ID_MDC_KEY, shardId)
            try {
                log.info("Reached shard end checkpointing.")
                shardEndedInput.checkpointer().checkpoint()
            } catch (e: Exception) {
                when (e) {
                    is ShutdownException, is InvalidStateException -> {
                        log.error("Exception while checkpointing at shard end. Giving up.", e)
                    }
                    else -> throw e
                }
            } finally {
                MDC.remove(SHARD_ID_MDC_KEY)
            }
        }

        override fun shutdownRequested(shutdownRequestedInput: ShutdownRequestedInput) {
            MDC.put(SHARD_ID_MDC_KEY, shardId)
            try {
                log.info("Scheduler is shutting down, checkpointing.")
                shutdownRequestedInput.checkpointer().checkpoint()
            } catch (e: Exception) {
                when (e) {
                    is ShutdownException, is InvalidStateException -> {
                        log.error("Exception while checkpointing at requested shutdown. Giving up.", e)
                    }
                    else -> throw e
                }
            } finally {
                MDC.remove(SHARD_ID_MDC_KEY)
            }
        }
    }
}