package com.yy.elasticsearch.priorstore;

import org.elasticsearch.cluster.routing.LocalShardStateAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

/**
 * @author colin.ke keqinwu@yy.com
 */
public class PriorStoreModule extends AbstractModule {

	public static final String MG_POLICY_KEY = "store.prior.migration.policy";

	/**
	 * if the size of the drive reach this threshold, data migration from prior-store path to non-prior-store path will be triggered
	 */
	public static final String MIGRATION_THRESHOLD = "store.prior.migration.threshold";
	public static final double DEFAULT_MIGRATION_THRESHOLD = 0.8;

	/**
	 * if the size of the drive reach this threshold, the policy of choosing the data path will be 'least_used'
	 *
	 * @see org.elasticsearch.index.store.distributor.LeastUsedDistributor
	 */
	public static final String PRIOR_STORE_MAX_THRESHOLD = "store.prior.max.threshold";
	public static final double DEFAULT_PRIOR_STORE_MAX_THRESHOLD = 0.85;

	private final Settings settings;

	@Inject
	public PriorStoreModule(Settings settings) {
		this.settings = settings;
	}

	@Override
	protected void configure() {
		bind(LocalShardStateAction.class).asEagerSingleton();
		if (Plugin.enabled()) {
			bind(PriorStoreEnvironment.class).asEagerSingleton();
			bind(MigrationPolicy.class).to(loadMgPolicy()).asEagerSingleton();
			bind(PriorStoreMigrationScheduler.class).asEagerSingleton();
		}
	}

	private Class<? extends MigrationPolicy> loadMgPolicy() {
		return settings.getAsClass(MG_POLICY_KEY, YYOLAPMigrationPolicy.class);
	}
}
