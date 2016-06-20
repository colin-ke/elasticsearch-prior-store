package com.yy.elasticsearch.priorstore;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

/**
 * @author colin.ke keqinwu@163.com
 */
public class YYOLAPMigrationPolicy implements MigrationPolicy {

	public static final String MIGRATION_PER = "store.prior.migration.policy.yyolap.per";
	public static final String MIGRATION_NONPARTION_THRESHOLD = "store.prior.migration.policy.yyolap.nonpartition.threshold";

	private final Settings settings;

	@Inject
	public YYOLAPMigrationPolicy(Settings settings) {
		this.settings = settings;
	}

	@Override
	public MigrationInfo getMigrationIndices(NodeEnvironment.NodePath nodePath) throws Exception {
		PriorityQueue<YYOLAPIndex> pq;
		final PriorityQueue<YYOLAPIndex> ptPQ = new PriorityQueue<>();
		final Set<String> nonPtIndices = new HashSet<>();
		final Map<String, Long> indicesSize = new HashMap<>();
		final int[] depth = new int[]{0};
		final String[] curIndex = new String[]{""};

		Files.walkFileTree(nodePath.indicesPath, new FileVisitor<Path>() {
			@Override
			public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
				if (depth[0] == 1) {
					curIndex[0] = dir.getFileName().toString();
					indicesSize.put(curIndex[0], 0L);
					if (dir.getFileName().toString().indexOf('@') != -1) {
						String[] pair = dir.getFileName().toString().split("@");
						ptPQ.offer(new YYOLAPIndex(pair[0], pair[1]));
					} else {
						nonPtIndices.add(dir.getFileName().toString());
					}
				}
				++depth[0];
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				if (attrs.isRegularFile()) {
					indicesSize.put(curIndex[0], indicesSize.get(curIndex[0]) + attrs.size());
				}
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
				--depth[0];
				return FileVisitResult.CONTINUE;
			}
		});

		double mgPer = settings.getAsDouble(MIGRATION_PER, 0.1);
		double nonPartitionThres = settings.getAsDouble(MIGRATION_NONPARTION_THRESHOLD, 0.2);
		long migrationSize = (long) (nodePath.fileStore.getTotalSpace() * mgPer); // 每次默认迁移10%的大小
		long nonPtSize = 0;
		for (String index : nonPtIndices) {
			nonPtSize += indicesSize.get(index);
		}
		if (nonPtSize > nodePath.fileStore.getTotalSpace() * nonPartitionThres) { //默认如果非分区索引占的空间超过20%, 则迁移非分区索引，且优先迁移占空间较大的
			pq = new PriorityQueue<>(11, new Comparator<YYOLAPIndex>() {
				@Override
				public int compare(YYOLAPIndex index1, YYOLAPIndex index2) {
					if (indicesSize.containsKey(index1.toString())) {
						if (indicesSize.containsKey(index2.toString())) {
							return indicesSize.get(index1.toString()).compareTo(indicesSize.get(index2.toString())) * -1;
						}
						return 1;
					} else if (indicesSize.containsKey(index2.toString())) {
						return -1;
					} else {
						return 0;
					}
				}
			});
			for (String nonPtIndex : nonPtIndices) {
				pq.offer(new YYOLAPIndex(nonPtIndex));
			}
		} else { // 否则迁移分区索引，优先迁移较旧的索引
			pq = ptPQ;
		}

		Set<String> mgIndices = new HashSet<>();
		long curSize = 0;
		while (!pq.isEmpty()) {
			YYOLAPIndex index = pq.poll();
			mgIndices.add(index.toString());
			curSize += indicesSize.get(index.toString());
			if (curSize >= migrationSize)
				break;
		}

		MigrationInfo mgInfo = new MigrationInfo();
		mgInfo.setIndices(mgIndices);
		mgInfo.setSize(curSize);
		return mgInfo;
	}

	static class YYOLAPIndex implements Comparable<YYOLAPIndex> {
		final String partition;
		final String baseIndex;

		public YYOLAPIndex(String index) {
			this(index, null);
		}

		public YYOLAPIndex(String baseIndex, String partition) {
			this.baseIndex = baseIndex;
			this.partition = partition;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (null == obj)
				return false;
			if (!(obj instanceof YYOLAPIndex))
				return false;
			YYOLAPIndex o = (YYOLAPIndex) obj;
			return o.partition.equals(this.partition) && o.baseIndex.equals(this.baseIndex);
		}

		@Override
		public String toString() {
			if (null != partition)
				return baseIndex + '@' + partition;
			else
				return baseIndex;
		}

		@Override
		public int compareTo(YYOLAPIndex pi) {
			if (this.partition.equals(pi.partition))
				return this.baseIndex.compareTo(pi.baseIndex);
			return this.partition.compareTo(pi.partition);
		}
	}
}
