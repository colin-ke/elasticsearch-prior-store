package com.yy.elasticsearch.priorstore;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.DirectoryUtils;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.distributor.AbstractDistributor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author colin.ke keqinwu@163.com
 */
public class PriorStoreDistributor extends AbstractDistributor {

	private PriorStoreEnvironment priorStoreEnv;
	private final Directory[] priorStoreDirs;

	@Inject
	public PriorStoreDistributor(DirectoryService directoryService, PriorStoreEnvironment psEnv, IndexStore indexStore) throws IOException {
		super(directoryService);
		Path[] locations = indexStore.shardIndexLocations(directoryService.shardId());
		assert delegates.length == locations.length : "delegates dir array's length should be the same as shard location array";

		NodeEnvironment.NodePath[] psPaths = psEnv.getPriorStorePaths();
		if (null == psPaths || psPaths.length == 0) {
			priorStoreDirs = null;
			return;
		}
		List<Directory> priorDirList = new ArrayList<>();
		for (NodeEnvironment.NodePath priorStorePath : psPaths) {
			for (int i = 0; i < locations.length; ++i) {
				if (locations[i].startsWith(priorStorePath.indicesPath))
					priorDirList.add(delegates[i]);
			}
		}
		priorStoreDirs = priorDirList.toArray(new Directory[priorDirList.size()]);
		this.priorStoreEnv = psEnv;
	}

	@Override
	protected Directory doAny() {
		if (null == priorStoreDirs || priorStoreDirs.length == 0)
			return doAnyDir(delegates, null);

		Directory directory;
		directory = doAnyDir(priorStoreDirs, priorStoreEnv.getPriorStoreMaxThreshold());
		if (null == directory)
			directory = doAnyDir(delegates, null);
		return directory;
	}

	public Directory doAnyDir(Directory[] dirs, Double threshold) {
		Directory directory = null;
		long size = Long.MIN_VALUE;
		int sameSize = 0;
		for (Directory dir : dirs) {
			long currentSize = getUsableSpace(dir);
			if (null != threshold) {
				double curPercent = 1.0 - (currentSize * 1.0 / getTotalSpace(dir));
				if (curPercent >= threshold)
					continue;
			}
			if (currentSize > size) {
				size = currentSize;
				directory = dir;
				sameSize = 1;
			} else if (currentSize == size) {
				sameSize++;
				// Ensure uniform distribution between all directories with the same size
				if (ThreadLocalRandom.current().nextDouble() < 1.0 / sameSize) {
					directory = dir;
				}
			}
		}
		return directory;
	}

	protected long getTotalSpace(Directory directory) {
		final FSDirectory leaf = DirectoryUtils.getLeaf(directory, FSDirectory.class);
		if (leaf != null) {
			return leaf.getDirectory().getTotalSpace();
		} else {
			return 0;
		}
	}

	@Override
	protected String name() {
		return "prior_store";
	}
}
