package com.yy.elasticsearch.priorstore;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.store.StoreModule;
import org.elasticsearch.plugins.AbstractPlugin;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;

/**
 * @author colin.ke keqinwu@yy.com
 */
public class Plugin extends AbstractPlugin {

	public static final String PRIOR_STORE_ENABLED_KEY = "store.prior.enabled";
	public static final String PRIOR_PATH_KEY = "data.prior.path";

	private static boolean enabled;

	public Plugin(Settings settings) {
		String priorPath = settings.get(PRIOR_PATH_KEY);
		enabled = settings.getAsBoolean(PRIOR_STORE_ENABLED_KEY, true) && null != priorPath && Files.isDirectory(Paths.get(priorPath));
	}

	@Override
	public String name() {
		return "prior-store";
	}

	@Override
	public String description() {
		return "make it possible to store in some data path(s) first";
	}

	@Override
	public Collection<Class<? extends Module>> modules() {
		return (Collection) Collections.singletonList(PriorStoreModule.class);
	}

	@Override
	public Settings additionalSettings() {
		if (enabled)
			return ImmutableSettings.builder().put(StoreModule.DISTIBUTOR_KEY, PriorStoreDistributor.class.getCanonicalName()).build();
		return ImmutableSettings.EMPTY;
	}

	public static boolean enabled() {
		return enabled;
	}
}
