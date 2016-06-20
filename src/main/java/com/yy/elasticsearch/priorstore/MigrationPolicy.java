package com.yy.elasticsearch.priorstore;

import org.elasticsearch.env.NodeEnvironment;

/**
 * @author colin.ke keqinwu@yy.com
 */
public interface MigrationPolicy {

	MigrationInfo getMigrationIndices(NodeEnvironment.NodePath nodePath) throws Exception;

}
