package com.yy.elasticsearch.priorstore;

import java.util.Set;

/**
 * @author colin.ke keqinwu@yy.com
 */
public class MigrationInfo {

	private Set<String> indices;
	private long size;

	public void setIndices(Set<String> indices) {
		this.indices = indices;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public Set<String> getIndices() {
		return indices;
	}

	public long getSize() {
		return size;
	}
}
