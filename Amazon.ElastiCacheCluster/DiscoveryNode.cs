﻿/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/apache2.0
 * 
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;

using Amazon.ElastiCacheCluster.Helpers;
using Amazon.ElastiCacheCluster.Operations;

using Enyim.Caching.Memcached;

namespace Amazon.ElastiCacheCluster
{
    /// <summary>
    /// A class that manages the discovery of endpoints inside of an ElastiCache cluster
    /// </summary>
    public class DiscoveryNode
    {
        #region Static ReadOnlys

        private static readonly Enyim.Caching.ILog log = Enyim.Caching.LogManager.GetLogger(typeof(DiscoveryNode));

        internal static readonly int DEFAULT_TRY_COUNT = 5;
        internal static readonly int DEFAULT_TRY_DELAY = 1000;

        #endregion

        /// <summary>
        /// The version of memcached running on the Nodes
        /// </summary>
        public Version NodeVersion { get; private set; }

        /// <summary>
        /// The version of the cluster configuration
        /// </summary>
        public int ClusterVersion { get; private set; }

        /// <summary>
        /// The number of nodes running inside of the cluster
        /// </summary>
        public int NodesInCluster { get { return nodes.Count; } }

        #region Private Fields

        private IPEndPoint EndPoint;

        private IMemcachedNode Node;

        private ElastiCacheClusterConfig config;

        private List<IMemcachedNode> nodes = new List<IMemcachedNode>();

        private ConfigurationPoller poller;

        private string hostname;
        private int port;

        private int tries;
        private int delay;

        private Object nodesLock, endpointLock, clusterLock;

        #endregion

        #region Constructors

        /// <summary>
        /// The node used to discover endpoints in an ElastiCache cluster
        /// </summary>
        /// <param name="config">The config of the client to access the SocketPool</param>
        /// <param name="hostname">The host name of the cluster with .cfg. in name</param>
        /// <param name="port">The port of the cluster</param>
        internal DiscoveryNode(ElastiCacheClusterConfig config, string hostname, int port)
            : this(config, hostname, port, DEFAULT_TRY_COUNT, DEFAULT_TRY_DELAY) { }

        /// <summary>
        /// The node used to discover endpoints in an ElastiCache cluster
        /// </summary>
        /// <param name="config">The config of the client to access the SocketPool</param>
        /// <param name="hostname">The host name of the cluster with .cfg. in name</param>
        /// <param name="port">The port of the cluster</param>
        /// <param name="tries">The number of tries for requesting config info</param>
        /// <param name="delay">The time, in miliseconds, to wait between tries</param>
        internal DiscoveryNode(ElastiCacheClusterConfig config, string hostname, int port, int tries, int delay)
        {
            #region Param Checks

            if (config == null)
                throw new ArgumentNullException("config");
            if (string.IsNullOrEmpty(hostname))
                throw new ArgumentNullException("hostname");
            if (port <= 0)
                throw new ArgumentException("Port cannot be 0 or less");
            if (tries < 1)
                throw new ArgumentException("Must atleast try once");
            if (delay < 0)
                throw new ArgumentException("The delay can't be negative");
            if (hostname.IndexOf(".cfg", StringComparison.OrdinalIgnoreCase) < 0)
                throw new ArgumentException("The hostname is not able to use Auto Discovery");

            #endregion

            #region Setting Members

            this.hostname = hostname;
            this.port = port;
            this.config = config;
            ClusterVersion = 0;
            this.tries = tries;
            this.delay = delay;

            clusterLock = new Object();
            endpointLock = new Object();
            nodesLock = new Object();

            #endregion

            ResolveEndPoint();
        }

        #endregion

        #region Poller Methods

        /// <summary>
        /// Used to start a poller that checks for changes in the cluster client configuration
        /// </summary>
        internal void StartPoller()
        {
            config.Pool.UpdateLocator(new List<IPEndPoint>(new IPEndPoint[] { EndPoint }));
            poller = new ConfigurationPoller(config);
            poller.StartTimer();
        }

        /// <summary>
        /// Used to start a poller that checks for changes in the cluster client configuration
        /// </summary>
        /// <param name="intervalDelay">Time between pollings, in miliseconds</param>
        internal void StartPoller(int intervalDelay)
        {
            poller = new ConfigurationPoller(config, intervalDelay);
            poller.StartTimer();
        }

        #endregion

        #region Config Info
        /// <summary>
        /// Parses the string NodeConfig into a list of IPEndPoints for configuration
        /// </summary>
        /// <returns>A list of IPEndPoints for config to use</returns>
        internal List<IPEndPoint> GetEndPointList()
        {
            try
            {
                var endpoints = AddrUtil.HashEndPointList(GetNodeConfig());

                lock (nodesLock)
                {
                    var nodesToRemove = new HashSet<IMemcachedNode>();
                    foreach (var node in nodes)
                    {
                        if (!endpoints.Contains(node.EndPoint))
                            nodesToRemove.Add(node);
                    }
                    foreach (var node in nodesToRemove)
                    {
                        nodes.Remove(node);
                    }

                    foreach (var point in endpoints)
                    {
                        if (nodes.FirstOrDefault(x => x.EndPoint.Equals(point)) == null)
                        {
                            nodes.Add(config.nodeFactory.CreateNode(point, config.SocketPool));
                        }
                    }
                }

                return endpoints;
            }
            catch (Exception ex)
            {
                // Error getting the list of endpoints. Most likely this is due to the
                // client being used outside of EC2. 
                log.Debug("Error getting endpoints list", ex);
                throw;
            }
        }

        /// <summary>
        /// Gets the Node configuration from "config get cluster" if it's new or "get AmazonElastiCache:cluster" if it's older than
        /// 1.4.14
        /// </summary>
        /// <returns>A string in the format "hostname1|ip1|port1 hostname2|ip2|port2 ..."</returns>
        internal string GetNodeConfig()
        {
            var tries = this.tries;
            var nodeVersion = GetNodeVersion();
            var older = new Version("1.4.14");
            var waiting = true;
            string message = "";
            string[] items = null;

            IGetOperation command = nodeVersion.CompareTo(older) < 0 ?
                                        command = new GetOperation("AmazonElastiCache:cluster") :
                                        command = new ConfigGetOperation("cluster");

            while (waiting && tries > 0)
            {
                tries--;
                try
                {
                    lock (nodesLock)
                    {
                        // This avoids timing out from requesting the config from the endpoint
                        foreach (var node in nodes.ToArray())
                        {
                            try
                            {
                                var result = node.Execute(command);

                                if (result.Success)
                                {
                                    var configCommand = command as IConfigOperation;
                                    items = Encoding.UTF8.GetString(configCommand.ConfigResult.Data.Array, configCommand.ConfigResult.Data.Offset, configCommand.ConfigResult.Data.Count).Split('\n');
                                    waiting = false;
                                    break;
                                }
                                else
                                {
                                    message = result.Message;
                                }
                            }
                            catch (Exception ex)
                            {
                                message = ex.Message;
                            }
                        }
                    }

                    if (waiting)
                        System.Threading.Thread.Sleep(delay);

                }
                catch (Exception ex)
                {
                    message = ex.Message;
                    System.Threading.Thread.Sleep(delay);
                }
            }

            if (waiting)
            {
                throw new TimeoutException(String.Format("Could not get config of version " + NodeVersion.ToString() + ". Tries: {0} Delay: {1}. " + message, this.tries, delay));
            }

            lock (clusterLock)
            {
                if (ClusterVersion < Convert.ToInt32(items[0]))
                    ClusterVersion = Convert.ToInt32(items[0]);
            }
            return items[1];
        }

        /// <summary>
        /// Finds the version of Memcached the Elasticache setup is running on
        /// </summary>
        /// <returns>Version of memcahced running on nodes</returns>
        internal Version GetNodeVersion()
        {
            if (NodeVersion != null)
            {
                return NodeVersion;
            }

            if (!string.IsNullOrEmpty(Node.ToString()) && Node.ToString().Equals("TestingAWSInternal"))
            {
                NodeVersion = new Version("1.4.14");
                return NodeVersion;
            }

            IStatsOperation statcommand = new Enyim.Caching.Memcached.Protocol.Text.StatsOperation(null);
            var statresult = Node.Execute(statcommand);

            string version;
            if (statcommand.Result != null && statcommand.Result.TryGetValue("version", out version))
            {
                NodeVersion = new Version(version);
                return NodeVersion;
            }
            else
            {
                log.Error("Could not call stats on Node endpoint");
                throw new CommandNotSupportedException("The node does not have a version in stats.");
            }
        }

        /// <summary>
        /// Tries to resolve the endpoint ip, used if the connection fails
        /// </summary>
        /// <returns>The resolved endpoint as an ip and port</returns>
        internal IPEndPoint ResolveEndPoint()
        {
            IPHostEntry entry = null;
            var waiting = true;
            var tryCount = tries;
            string message = "";

            while (tryCount > 0 && waiting)
            {
                try
                {
                    tryCount--;
                    entry = Dns.GetHostEntryAsync(hostname).Result;
                    if (entry.AddressList.Length > 0)
                    {
                        waiting = false;
                    }
                }
                catch (Exception ex)
                {
                    message = ex.Message;
                    System.Threading.Thread.Sleep(delay);
                }
            }


            if (waiting || entry == null)
            {
                log.Error("Could not resolve hostname to ip");
                throw new TimeoutException(String.Format("Could not resolve hostname to Ip after trying the specified amount: {0}. " + message, tries));
            }

            log.DebugFormat("Resolved configuration endpoint {0} to {1}.", hostname, entry.AddressList[0]);

            lock (endpointLock)
            {
                EndPoint = new IPEndPoint(entry.AddressList[0], port);
            }

            lock (nodesLock)
            {
                if (Node != null)
                {
                    try
                    {
                        Node.Dispose();
                    }
                    catch { }
                }
                Node = config.nodeFactory.CreateNode(EndPoint, config.SocketPool);
                nodes.Clear();
                nodes.Add(Node);
            }
            return EndPoint;
        }

        #endregion

        /// <summary>
        /// Stops the current poller
        /// </summary>
        public void Dispose()
        {
            if (poller != null)
                poller.StopPolling();
        }
    }
}
