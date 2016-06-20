package com.yy.elasticsearch.priorstore;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * @author colin.ke keqinwu@163.com
 */
public class PriorStoreEnvironment {

	private final Settings settings;
	private final NodeEnvironment.NodePath[] priorStorePaths;

	@Inject
	public PriorStoreEnvironment(NodeEnvironment nodeEnv, Settings settings) {
		NodeEnvironment.NodePath[] nodePaths = nodeEnv.nodePaths();
		String[] priorStoreSetting = settings.getAsArray(Plugin.PRIOR_PATH_KEY);
		List<NodeEnvironment.NodePath> tmp = new ArrayList<>();
		for (String psPathStr : priorStoreSetting) {
			Path psPath = Paths.get(psPathStr);
			for (NodeEnvironment.NodePath nodePath : nodePaths) {
				if (nodePath.path.startsWith(psPath)) {
					tmp.add(nodePath);
					break;
				}
			}
		}
		priorStorePaths = tmp.toArray(new NodeEnvironment.NodePath[tmp.size()]);
		this.settings = settings;
	}

	public NodeEnvironment.NodePath[] getPriorStorePaths() {
		return priorStorePaths;
	}

	public double getPriorStoreMaxThreshold() {
		return settings.getAsDouble(PriorStoreModule.PRIOR_STORE_MAX_THRESHOLD, PriorStoreModule.DEFAULT_PRIOR_STORE_MAX_THRESHOLD);
	}

	public double getMigrationThreshold() {
		return settings.getAsDouble(PriorStoreModule.MIGRATION_THRESHOLD, PriorStoreModule.DEFAULT_MIGRATION_THRESHOLD);
	}
}
