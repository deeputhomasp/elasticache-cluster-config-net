/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Portions copyright 2010 Attila Kiskó, enyim.com. Please see LICENSE.txt
 * for applicable license terms and NOTICE.txt for applicable notices.
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
using System.Threading;

using Enyim.Caching.Configuration;
using Enyim.Caching.Memcached;
using Microsoft.Extensions.Logging;

namespace Amazon.ElastiCacheCluster.Pools
{
    /// <summary>
    /// A server pool just like the default that enables safely changing the servers of the locator
    /// </summary>
    internal class AutoServerPool : IServerPool, IDisposable
    {
        private static readonly Enyim.Caching.ILog Log = Enyim.Caching.LogManager.GetLogger(typeof(DefaultServerPool));

        private static readonly ILogger<AutoServerPool> Logger = new LoggerFactory().CreateLogger<AutoServerPool>();

        private IMemcachedNode[] allNodes;

        private IMemcachedClientConfiguration configuration;
        private IOperationFactory factory;
        internal IMemcachedNodeLocator nodeLocator;

        private object DeadSync = new Object();
        private Timer resurrectTimer;
        private bool isTimerActive;
        private int deadTimeoutMsec;
        private bool isDisposed;
        private event Action<IMemcachedNode> nodeFailed;

        /// <summary>
        /// Creates a server pool for auto discovery
        /// </summary>
        /// <param name="configuration">The client configuration using the pool</param>
        /// <param name="opFactory">The factory used to create operations on demand</param>
        public AutoServerPool(IMemcachedClientConfiguration configuration, IOperationFactory opFactory)
        {
            if (configuration == null) throw new ArgumentNullException("socketConfig");
            if (opFactory == null) throw new ArgumentNullException("opFactory");

            this.configuration = configuration;
            factory = opFactory;

            deadTimeoutMsec = (int)this.configuration.SocketPool.DeadTimeout.TotalMilliseconds;
        }

        ~AutoServerPool()
        {
            try { ((IDisposable)this).Dispose(); }
            catch { }
        }

        protected virtual IMemcachedNode CreateNode(EndPoint endpoint)
        {
            return new MemcachedNode(endpoint, configuration.SocketPool, Logger);
        }

        private void rezCallback(object state)
        {
            var isDebug = Log.IsDebugEnabled;

            if (isDebug) Log.Debug("Checking the dead servers.");

            // how this works:
            // 1. timer is created but suspended
            // 2. Locate encounters a dead server, so it starts the timer which will trigger after deadTimeout has elapsed
            // 3. if another server goes down before the timer is triggered, nothing happens in Locate (isRunning == true).
            //		however that server will be inspected sooner than Dead Timeout.
            //		   S1 died   S2 died    dead timeout
            //		|----*--------*------------*-
            //           |                     |
            //          timer start           both servers are checked here
            // 4. we iterate all the servers and record it in another list
            // 5. if we found a dead server whihc responds to Ping(), the locator will be reinitialized
            // 6. if at least one server is still down (Ping() == false), we restart the timer
            // 7. if all servers are up, we set isRunning to false, so the timer is suspended
            // 8. GOTO 2
            lock (DeadSync)
            {
                if (isDisposed)
                {
                    if (Log.IsWarnEnabled) Log.Warn("IsAlive timer was triggered but the pool is already disposed. Ignoring.");

                    return;
                }

                var nodes = allNodes;
                var aliveList = new List<IMemcachedNode>(nodes.Length);
                var changed = false;
                var deadCount = 0;

                for (var i = 0; i < nodes.Length; i++)
                {
                    var n = nodes[i];
                    if (n.IsAlive)
                    {
                        if (isDebug) Log.DebugFormat("Alive: {0}", n.EndPoint);

                        aliveList.Add(n);
                    }
                    else
                    {
                        if (isDebug) Log.DebugFormat("Dead: {0}", n.EndPoint);

                        if (n.Ping())
                        {
                            changed = true;
                            aliveList.Add(n);

                            if (isDebug) Log.Debug("Ping ok.");
                        }
                        else
                        {
                            if (isDebug) Log.Debug("Still dead.");

                            deadCount++;
                        }
                    }
                }

                // reinit the locator
                if (changed)
                {
                    if (isDebug) Log.Debug("Reinitializing the locator.");

                    nodeLocator.Initialize(aliveList);
                }

                // stop or restart the timer
                if (deadCount == 0)
                {
                    if (isDebug) Log.Debug("deadCount == 0, stopping the timer.");

                    isTimerActive = false;
                }
                else
                {
                    if (isDebug) Log.DebugFormat("deadCount == {0}, starting the timer.", deadCount);

                    resurrectTimer.Change(deadTimeoutMsec, Timeout.Infinite);
                }
            }
        }

        private void NodeFail(IMemcachedNode node)
        {
            var isDebug = Log.IsDebugEnabled;
            if (isDebug) Log.DebugFormat("Node {0} is dead.", node.EndPoint);

            // the timer is stopped until we encounter the first dead server
            // when we have one, we trigger it and it will run after DeadTimeout has elapsed
            lock (DeadSync)
            {
                if (isDisposed)
                {
                    if (Log.IsWarnEnabled) Log.Warn("Got a node fail but the pool is already disposed. Ignoring.");

                    return;
                }

                // bubble up the fail event to the client
                var fail = nodeFailed;
                if (fail != null)
                    fail(node);

                // re-initialize the locator
                var newLocator = configuration.CreateNodeLocator();
                newLocator.Initialize(allNodes.Where(n => n.IsAlive).ToArray());
                Interlocked.Exchange(ref nodeLocator, newLocator);

                // the timer is stopped until we encounter the first dead server
                // when we have one, we trigger it and it will run after DeadTimeout has elapsed
                if (!isTimerActive)
                {
                    if (isDebug) Log.Debug("Starting the recovery timer.");

                    if (resurrectTimer == null)
                        resurrectTimer = new Timer(rezCallback, null, deadTimeoutMsec, Timeout.Infinite);
                    else
                        resurrectTimer.Change(deadTimeoutMsec, Timeout.Infinite);

                    isTimerActive = true;

                    if (isDebug) Log.Debug("Timer started.");
                }
            }
        }

        #region [ IServerPool                  ]

        IMemcachedNode IServerPool.Locate(string key)
        {
            var node = nodeLocator.Locate(key);

            return node;
        }

        IOperationFactory IServerPool.OperationFactory
        {
            get { return factory; }
        }

        IEnumerable<IMemcachedNode> IServerPool.GetWorkingNodes()
        {
            return nodeLocator.GetWorkingNodes();
        }

        void IServerPool.Start()
        {
            allNodes = configuration.Servers.
                                Select(ip =>
                                {
                                    var node = this.CreateNode(ip);
                                    node.Failed += NodeFail;

                                    return node;
                                }).
                                ToArray();

            // initialize the locator
            var locator = configuration.CreateNodeLocator();
            locator.Initialize(allNodes);

            nodeLocator = locator;

            var config = configuration as ElastiCacheClusterConfig;
            if (config.setup.ClusterPoller.IntervalDelay < 0)
                config.DiscoveryNode.StartPoller();
            else
                config.DiscoveryNode.StartPoller(config.setup.ClusterPoller.IntervalDelay);
        }

        event Action<IMemcachedNode> IServerPool.NodeFailed
        {
            add { nodeFailed += value; }
            remove { nodeFailed -= value; }
        }

        #endregion
        #region [ IDisposable                  ]

        void IDisposable.Dispose()
        {
            GC.SuppressFinalize(this);

            lock (DeadSync)
            {
                if (isDisposed) return;

                isDisposed = true;

                // dispose the locator first, maybe it wants to access 
                // the nodes one last time
                var nd = nodeLocator as IDisposable;
                if (nd != null)
                    try { nd.Dispose(); }
                    catch (Exception e) { if (Log.IsErrorEnabled) Log.Error(e); }

                nodeLocator = null;

                for (var i = 0; i < allNodes.Length; i++)
                    try { allNodes[i].Dispose(); }
                    catch (Exception e) { if (Log.IsErrorEnabled) Log.Error(e); }

                // stop the timer
                if (resurrectTimer != null)
                    using (resurrectTimer)
                        resurrectTimer.Change(Timeout.Infinite, Timeout.Infinite);

                allNodes = null;
                resurrectTimer = null;
            }
        }

        #endregion

        /// <summary>
        /// Used to update the servers for Auto discovery
        /// </summary>
        /// <param name="endPoints">The connections to all the cluster nodes</param>
        public void UpdateLocator(List<IPEndPoint> endPoints)
        {
            var newLocator = configuration.CreateNodeLocator();
            newLocator.Initialize(endPoints.Select(ip =>
            {
                var node = CreateNode(ip);
                node.Failed += NodeFail;

                return node;
            }).ToArray());

            Interlocked.Exchange(ref nodeLocator, newLocator);
        }
    }
}
