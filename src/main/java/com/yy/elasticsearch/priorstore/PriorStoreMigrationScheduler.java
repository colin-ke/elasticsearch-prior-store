package com.yy.elasticsearch.priorstore;

import com.google.common.primitives.Ints;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.LocalShardStateAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.support.AbstractIndexStore;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author colin.ke keqinwu@163.com
 */
public class PriorStoreMigrationScheduler {

	public static String MIGRATION_SCHEDULE_INTERVAL = "store.prior.migration.schedule.interval";
	public static String MIGRATION_CONCURRENT = "store.prior.migration.concurrent"; // default 5

	private static AtomicBoolean running = new AtomicBoolean(false);

	private final PriorStoreEnvironment psEnv;
	private final NodeEnvironment nodeEnv;
	private final MigrationPolicy mgPolicy;
	private final IndicesService indicesService;
	private final LocalShardStateAction shardStateAction;
	private final Semaphore migrating;

	private final ESLogger logger = Loggers.getLogger(getClass());
	private final Node node;

	@Inject
	public PriorStoreMigrationScheduler(final ThreadPool threadPool, final NodeEnvironment nodeEnv, final PriorStoreEnvironment psEnv, final MigrationPolicy mgPolicy,
										final IndicesService indicesService, LocalShardStateAction shardStateAction, Settings settings, Node node) {
		this.nodeEnv = nodeEnv;
		this.psEnv = psEnv;
		this.mgPolicy = mgPolicy;
		this.indicesService = indicesService;
		this.shardStateAction = shardStateAction;
		this.node = node;

		migrating = new Semaphore(settings.getAsInt(MIGRATION_CONCURRENT, 5));
		TimeValue interval = settings.getAsTime(MIGRATION_SCHEDULE_INTERVAL, TimeValue.timeValueHours(1));
		threadPool.scheduleWithFixedDelay(migrationRunnable, interval);
	}

	private final Runnable migrationRunnable = new Runnable() {
		@Override
		public void run() {
			try {
				if (running.getAndSet(true))
					return; // if it's already running and haven't finished yet, return;

				// 1. start running migration checking runnable
				if (null == psEnv.getPriorStorePaths() || psEnv.getPriorStorePaths().length == 0)
					return;
				for (NodeEnvironment.NodePath psNodePath : psEnv.getPriorStorePaths()) {
					if ((psNodePath.fileStore.getUsableSpace() * 1.0 / psNodePath.fileStore.getTotalSpace()) > (1.0 - psEnv.getMigrationThreshold()))
						continue;

					MigrationInfo mgInfo = mgPolicy.getMigrationIndices(psNodePath);
					NodeEnvironment.NodePath target = null;
					for (NodeEnvironment.NodePath nodePath : nodeEnv.nodePaths()) { // 目标路径跟源路径不是同一个硬盘，且目标硬盘的可用空间最大
						if (nodePath.fileStore.equals(psNodePath.fileStore) || nodePath.fileStore.getUsableSpace() < mgInfo.getSize())
							continue;
						if (null == target)
							target = nodePath;
						else if (nodePath.fileStore.getUsableSpace() > target.fileStore.getUsableSpace())
							target = nodePath;
					}
					if (null != target) {
						for (String index : mgInfo.getIndices()) {
//							logger.debug("starting migrating index [{}] from {} to {}", index, psNodePath.path, target.path);
							ClusterHealthStatus indexStatus = node.client().admin().cluster().prepareHealth(index).setLocal(true).execute().actionGet().getStatus();
							if(indexStatus.equals(ClusterHealthStatus.GREEN))
								migrateIndex(index, psNodePath, target);
							else
								logger.debug("index[{}] status is {}, ignore migration", index, indexStatus);
						}
					} else {
						logger.warn("no target store can be migrated to from " + psNodePath.path.toString());
					}
				}
			} catch (Exception e) {
				throw new IllegalStateException("failed to migrate", e);
			} finally {
				running.set(false);
			}
		}
	};

	private void migrateIndex(final String index, final NodeEnvironment.NodePath src, final NodeEnvironment.NodePath dst) throws IOException, InterruptedException {
		logger.info("migrating index [{}] from {} to {}", index, src, dst);
		// 2. migrate index - find all the shards in src path
		Set<ShardId> shardIds = findAllShardsForIndex(src.indicesPath.resolve(index));
		if (null == indicesService) {
			// should not go here, just in case
			logger.error("cannot get indexService for {}", index);
			return;
		}


		for (final ShardId shardId : shardIds) {
			if(!migrating.tryAcquire(1, TimeUnit.HOURS)) {
				logger.error("acquire 'migrating' semaphore timeout(1 hour) !!");
			}

			logger.info("migrating shard[{}]", shardId);

			// 3. unassign the shards in src path(release shard lock)
			shardStateAction.unassignShards(new ShardId[]{shardId}, new LocalShardStateAction.Listener() {
				@Override
				public void onResponse(LocalShardStateAction.ShardStateResponse response) {
					if (!response.ack()) {
						logger.error("failed to remove shards {} with ack returned false", shardId);
						migrating.release();
						throw new IllegalStateException("failed to migrate: remove shards action returned false ack");
					}

					final Boolean[] hasError = new Boolean[]{false};
					// 4. obtain shard lock
					try (ShardLock shardLock = nodeEnv.shardLock(shardId, 60)) {
						// do migrating shard file
						Path srcShard = src.indicesPath.resolve(index).resolve(String.valueOf(shardId.id()));
						Path dstShard = dst.indicesPath.resolve(index).resolve(String.valueOf(shardId.id()));
						Files.createDirectories(dstShard.resolve(AbstractIndexStore.INDEX_FOLDER_NAME));
						Files.createDirectories(dstShard.resolve(AbstractIndexStore.TRANSLOG_FOLDER_NAME));
						migrateDir(shardId, srcShard.resolve(AbstractIndexStore.INDEX_FOLDER_NAME), dstShard.resolve(AbstractIndexStore.INDEX_FOLDER_NAME));
						migrateDir(shardId, srcShard.resolve(AbstractIndexStore.TRANSLOG_FOLDER_NAME), dstShard.resolve(AbstractIndexStore.TRANSLOG_FOLDER_NAME));
					} catch (LockObtainFailedException e) {
						hasError[0] = true;
						throw new IllegalStateException("failed to obtain shard lock [{" + shardId + "}]", e);
					} catch (Exception e) {
						hasError[0] = true;
						throw new IllegalStateException(e);
					} finally {
						// 5. re-assign the shard to local node
						shardStateAction.startShards(new ShardId[]{shardId}, new LocalShardStateAction.Listener() {
							@Override
							public void onResponse(LocalShardStateAction.ShardStateResponse response) {
								migrating.release();
								if (!response.ack()) {
									logger.error("failed to start shards {} with ack returned false", shardId);
									throw new IllegalStateException("start shards action returned false ack");
								}
								if (hasError[0])
									logger.info("restarted shard {} without migrating any data", shardId);
								else
									logger.info("successfully migrating shard[{}]", shardId);
							}

							@Override
							public void onFailure(Throwable e) {
								migrating.release();
								throw new IllegalStateException("", e);
							}
						});
					}
				}

				@Override
				public void onFailure(Throwable e) {
					logger.error("failed to unassign the shard[{}]", e, shardId);
					migrating.release();
					throw new IllegalStateException(e);
				}
			});
		}
	}

	private void migrateDir(ShardId shard, Path sourceDir, Path targetDir) throws IOException {
		List<Path> movedFiles = new ArrayList<>();

		if (Files.exists(sourceDir)) {
			logger.debug("{} migrating from [{}] to [{}]", shard, sourceDir, targetDir);
			try (DirectoryStream<Path> stream = Files.newDirectoryStream(sourceDir)) {
				Files.createDirectories(targetDir);
				for (Path file : stream) {
					if (IndexWriter.WRITE_LOCK_NAME.equals(file.getFileName().toString()) || IndexFileNames.SEGMENTS_GEN.equals(file.getFileName().toString()) || Files.isDirectory(file)) {
						continue; // skip write.lock and segments.gen
					}
					logger.trace("{} move file [{}] size: [{}]", shard, file.getFileName(), Files.size(file));
					final Path targetFile = targetDir.resolve(file.getFileName());
					/* We are pessimistic and do a copy first to the other path and then and atomic move to rename it such that
					in the worst case the file exists twice but is never lost or half written.*/
					final Path targetTempFile = Files.createTempFile(targetDir, "upgrade_", "_" + file.getFileName().toString());
					Files.copy(file, targetTempFile, StandardCopyOption.COPY_ATTRIBUTES, StandardCopyOption.REPLACE_EXISTING);
					Files.move(targetTempFile, targetFile, StandardCopyOption.ATOMIC_MOVE); // we are on the same FS - this must work otherwise all bets are off
					Files.delete(file);
					movedFiles.add(targetFile);
				}
			}
		}

		if (!movedFiles.isEmpty()) {
			// fsync later it might be on disk already
			logger.trace("{} fsync files", shard);
			for (Path moved : movedFiles) {
				logger.trace("{} syncing [{}]", shard, moved.getFileName());
				IOUtils.fsync(moved.toFile(), false);
			}
			logger.trace("{} syncing directory [{}]", shard, targetDir);
			IOUtils.fsync(targetDir.toFile(), true);
		}
	}


	private static Set<ShardId> findAllShardsForIndex(Path indexPath) throws IOException {
		Set<ShardId> shardIds = new HashSet<>();
		if (Files.isDirectory(indexPath)) {
			try (DirectoryStream<Path> stream = Files.newDirectoryStream(indexPath)) {
				String currentIndex = indexPath.getFileName().toString();
				for (Path shardPath : stream) {
					if (Files.isDirectory(shardPath)) {
						Integer shardId = Ints.tryParse(shardPath.getFileName().toString());
						if (shardId != null) {
							ShardId id = new ShardId(currentIndex, shardId);
							shardIds.add(id);
						}
					}
				}
			}
		}
		return shardIds;
	}
}
