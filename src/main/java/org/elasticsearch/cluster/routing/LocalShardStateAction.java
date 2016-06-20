package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.*;

/**
 * @author colin.ke keqinwu@yy.com
 */
public class LocalShardStateAction extends AbstractComponent {

	public static final String START_SHARD_ACTION_NAME = "internal:cluster/shard/localStart";
	public static final String UNASSIGN_SHARD_ACTION_NAME = "internal:cluster/shard/localUnassign";
	private final ClusterService clusterService;
	private final TransportService transportService;

	@Inject
	public LocalShardStateAction(Settings settings, TransportService transportService, ClusterService clusterService) {
		super(settings);
		this.transportService = transportService;
		this.clusterService = clusterService;
		transportService.registerHandler(START_SHARD_ACTION_NAME, new StartShardHandler());
		transportService.registerHandler(UNASSIGN_SHARD_ACTION_NAME, new UnassignShardHandler());
	}

	public void startShards(ShardId[] shardIds, Listener listener) {
		sendRequest(START_SHARD_ACTION_NAME, shardIds, listener);
	}

	public void unassignShards(ShardId[] shardIds, Listener listener) {
		sendRequest(UNASSIGN_SHARD_ACTION_NAME, shardIds, listener);
	}

	private void sendRequest(String action, ShardId[] shardIds, final Listener listener) {
		ClusterState clusterState = clusterService.state();
		DiscoveryNode masterNode = clusterState.nodes().masterNode();
		if (masterNode == null) {
			listener.onFailure(new MasterNotDiscoveredException("no master to send request for action: " + action));
			return;
		}
		TransportRequest request = new ChangeShardStateRequest(clusterState.nodes().localNode(), shardIds);
		transportService.sendRequest(masterNode, action, request, new TransportResponseHandler<ShardStateResponse>() {

			@Override
			public ShardStateResponse newInstance() {
				return new ShardStateResponse();
			}

			@Override
			public void handleResponse(ShardStateResponse response) {
				listener.onResponse(response);
			}

			@Override
			public void handleException(TransportException exp) {
				listener.onFailure(exp);
			}

			@Override
			public String executor() {
				return ThreadPool.Names.SAME;
			}
		});
	}


	class UnassignShardHandler extends BaseTransportRequestHandler<ChangeShardStateRequest> {

		@Override
		public ChangeShardStateRequest newInstance() {
			return new ChangeShardStateRequest();
		}

		@Override
		public void messageReceived(final ChangeShardStateRequest request, final TransportChannel channel) throws Exception {
			clusterService.submitStateUpdateTask("remove shards " + Arrays.toString(request.shardIds), Priority.URGENT, new ProcessedClusterStateUpdateTask() {

				@Override
				public ClusterState execute(ClusterState currentState) throws Exception {

					RoutingTable.Builder rtBuilder = RoutingTable.builder(currentState.routingTable());
					// it returns a new RoutingNodes object
					RoutingNodes nodes = currentState.routingNodes();
					RoutingNode node = nodes.node(request.fromNode.id());
					Set<ShardId> shardIdSet = new HashSet<>(Arrays.asList(request.shardIds));

					for (MutableShardRouting sr : node) {

						if (!shardIdSet.contains(sr.shardId()) || !request.fromNode.id().equals(sr.currentNodeId))
							continue;
						if (sr.primary()) {
							MutableShardRouting replica = nodes.activeReplica(sr);
							if (null != replica) {
								nodes.swapPrimaryFlag(replica);
								nodes.swapPrimaryFlag(sr);
							}
						}
						sr.moveToUnassigned(new UnassignedInfo(UnassignedInfo.Reason.UNKNOWN, "for prior store migration"));
					}

					return ClusterState.builder(currentState).routingTable(rtBuilder.updateNodes(nodes)).build();
				}

				@Override
				public void onFailure(String source, Throwable t) {
					logger.error("unexpected failure during [{}]", t, source);
					try {
						channel.sendResponse(t);
					} catch (Exception e) {
						logger.warn("failed send response for removing shard", e);
					}
				}

				@Override
				public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
					try {
						channel.sendResponse(new ShardStateResponse(true));
					} catch (IOException e) {
						logger.warn("failed send response for removing shards", e);
					}
				}
			});
		}

		@Override
		public String executor() {
			return ThreadPool.Names.SAME;
		}
	}

	class StartShardHandler extends BaseTransportRequestHandler<ChangeShardStateRequest> {

		@Override
		public ChangeShardStateRequest newInstance() {
			return new ChangeShardStateRequest();
		}

		@Override
		public void messageReceived(final ChangeShardStateRequest request, final TransportChannel channel) throws Exception {
			clusterService.submitStateUpdateTask("reinitialize shards " + Arrays.toString(request.shardIds), Priority.URGENT, new ProcessedClusterStateUpdateTask() {

				@Override
				public ClusterState execute(ClusterState currentState) throws Exception {

					RoutingTable.Builder rtBuilder = RoutingTable.builder(currentState.routingTable());
					// warn: it returns a new RoutingNodes object
					RoutingNodes nodes = currentState.routingNodes();
					Set<ShardId> shardIdSet = new HashSet<>(Arrays.asList(request.shardIds));
					Map<ShardId, MutableShardRouting> map = new HashMap<>();
					for (MutableShardRouting sr : nodes.unassigned()) {
						if (shardIdSet.contains(sr.shardId())) {
							if (!map.containsKey(sr.shardId()) || sr.primary())
								map.put(sr.shardId(), sr);
						}
					}

					for (Map.Entry<ShardId, MutableShardRouting> entry : map.entrySet()) {
						nodes.assign(entry.getValue(), request.fromNode.id());
					}

					return ClusterState.builder(currentState).routingTable(rtBuilder.updateNodes(nodes)).build();
				}

				@Override
				public void onFailure(String source, Throwable t) {
					logger.error("unexpected failure during [{}]", t, source);
					try {
						channel.sendResponse(t);
					} catch (Exception e) {
						logger.warn("failed send response for re-init shards", e);
					}
				}

				@Override
				public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
					try {
						channel.sendResponse(new ShardStateResponse(true));
					} catch (IOException e) {
						logger.warn("failed send response for re-init shards", e);
					}
				}
			});
		}

		@Override
		public String executor() {
			return ThreadPool.Names.SAME;
		}
	}

	static class ChangeShardStateRequest extends TransportRequest {

		DiscoveryNode fromNode;
		ShardId[] shardIds;

		public ChangeShardStateRequest() {

		}

		public ChangeShardStateRequest(DiscoveryNode node, ShardId... shardIds) {
			this.fromNode = node;
			this.shardIds = shardIds;
		}

		@Override
		public void readFrom(StreamInput in) throws IOException {
			super.readFrom(in);
			fromNode = DiscoveryNode.readNode(in);
			shardIds = new ShardId[in.readVInt()];
			for (int i = 0; i < shardIds.length; ++i) {
				shardIds[i] = ShardId.readShardId(in);
			}
		}

		@Override
		public void writeTo(StreamOutput out) throws IOException {
			super.writeTo(out);
			fromNode.writeTo(out);
			out.writeVInt(shardIds.length);
			for (ShardId shardId : shardIds) {
				shardId.writeTo(out);
			}
		}
	}

	public static class ShardStateResponse extends TransportResponse {

		private boolean ack;

		ShardStateResponse() {
		}

		ShardStateResponse(boolean ack) {
			this.ack = ack;
		}

		public boolean ack() {
			return ack;
		}

		@Override
		public void readFrom(StreamInput in) throws IOException {
			super.readFrom(in);
			ack = in.readBoolean();
		}

		@Override
		public void writeTo(StreamOutput out) throws IOException {
			super.writeTo(out);
			out.writeBoolean(ack);
		}
	}

	public interface Listener {
		void onResponse(ShardStateResponse response);

		void onFailure(Throwable e);
	}
}
