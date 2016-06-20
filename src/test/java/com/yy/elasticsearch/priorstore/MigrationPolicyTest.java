package com.yy.elasticsearch.priorstore;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.env.NodeEnvironment;

import java.nio.file.Paths;

/**
 * @author colin.ke keqinwu@yy.com
 */
public class MigrationPolicyTest {

	public static void main(String[] args) throws Exception {
		ImmutableSettings.Builder builder = ImmutableSettings.builder();
		builder.put(YYOLAPMigrationPolicy.MIGRATION_NONPARTION_THRESHOLD, 0.0);
//		builder.put(YYOLAPMigrationPolicy.MIGRATION_PER, 0.1);
		MigrationPolicy policy = new YYOLAPMigrationPolicy(builder.build());

		NodeEnvironment.NodePath nodePath = new NodeEnvironment.NodePath(Paths.get("D:\\home\\myes\\data\\dev\\nodes\\0"));
		MigrationInfo info = policy.getMigrationIndices(nodePath);
		System.out.println("indices:");
		for (String index : info.getIndices())
			System.out.println(index);
		System.out.println("size: " + (info.getSize() / 1024 / 1024));
	}

}
