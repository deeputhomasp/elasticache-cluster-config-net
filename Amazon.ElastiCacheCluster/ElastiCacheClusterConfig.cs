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
using System.IO;
using System.Net;

using Amazon.ElastiCacheCluster.Factories;
using Amazon.ElastiCacheCluster.Pools;

using Enyim.Caching.Configuration;
using Enyim.Caching.Memcached;
using Enyim.Reflection;

using Microsoft.Extensions.Configuration;

namespace Amazon.ElastiCacheCluster
{
    /// <summary>
    /// Configuration class for auto discovery
    /// </summary>
    public class ElastiCacheClusterConfig : IMemcachedClientConfiguration
    {
        private static IConfigurationRoot _configuration;

        // these are lazy initialized in the getters
        private Type nodeLocator;
        private ITranscoder transcoder;
        private IMemcachedKeyTransformer keyTransformer;

        internal ClusterConfigSettings setup;
        internal AutoServerPool Pool;
        internal IConfigNodeFactory nodeFactory;

        /// <summary>
        /// The node used to check the cluster's configuration
        /// </summary>
        public DiscoveryNode DiscoveryNode { get; private set; }

        #region Constructors

        static ElastiCacheClusterConfig()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json");
            _configuration = builder.Build();
        }

        /// <summary>
        /// Initializes a MemcahcedClient config with auto discovery enabled from the app.config clusterclient section
        /// </summary>
        public ElastiCacheClusterConfig()
            : this(null as ClusterConfigSettings) { }

        /// <summary>
        /// Initializes a MemcahcedClient config with auto discovery enabled from the app.config with the specified section
        /// </summary>
        /// <param name="section">The section to get config settings from</param>
        public ElastiCacheClusterConfig(string section)
            : this(_configuration.GetSection(section) as ClusterConfigSettings) { }

        /// <summary>
        /// Initializes a MemcahcedClient config with auto discovery enabled
        /// </summary>
        /// <param name="hostname">The hostname of the cluster containing ".cfg."</param>
        /// <param name="port">The port to connect to for communication</param>
        public ElastiCacheClusterConfig(string hostname, int port)
            : this(new ClusterConfigSettings(hostname, port)) { }

        /// <summary>
        /// Initializes a MemcahcedClient config with auto discovery enabled using the setup provided
        /// </summary>
        /// <param name="setup">The setup to get conifg settings from</param>
        public ElastiCacheClusterConfig(ClusterConfigSettings setup)
        {
            if (setup == null)
            {
                try
                {
                    setup = _configuration.GetSection("clusterclient") as ClusterConfigSettings;
                    if (setup == null)
                    {
                        throw new Exception("Could not instantiate from appsettings.json, setup was null");
                    }
                }
                catch (Exception ex)
                {
                    throw new Exception("Could not instantiate from appsettings.json\n" + ex.Message);
                }
            }

            if (setup.ClusterEndPoint == null)
                throw new ArgumentException("Cluster Settings are null");
            if (string.IsNullOrEmpty(setup.ClusterEndPoint.HostName))
                throw new ArgumentNullException("hostname");
            if (setup.ClusterEndPoint.Port <= 0)
                throw new ArgumentException("Port cannot be 0 or less");

            this.setup = setup;
            Servers = new List<EndPoint>();

            Protocol = setup.Protocol;

            if (setup.KeyTransformer == null)
                KeyTransformer = new DefaultKeyTransformer();
            else
                KeyTransformer = setup.KeyTransformer.CreateInstance() ?? new DefaultKeyTransformer();

            SocketPool = (ISocketPoolConfiguration)setup.SocketPool ?? new SocketPoolConfiguration();
            Authentication = (IAuthenticationConfiguration)setup.Authentication ?? new AuthenticationConfiguration();

            nodeFactory = setup.NodeFactory ?? new DefaultConfigNodeFactory();
            nodeLocator = setup.NodeLocator != null ? setup.NodeLocator.Type : typeof(DefaultNodeLocator);
            
            if (setup.Transcoder != null)
            {
                transcoder = setup.Transcoder.CreateInstance() ?? new DefaultTranscoder();
            }
            
            if (setup.ClusterEndPoint.HostName.IndexOf(".cfg", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                if (setup.ClusterNode != null)
                {
                    var _tries = setup.ClusterNode.NodeTries > 0 ? setup.ClusterNode.NodeTries : DiscoveryNode.DEFAULT_TRY_COUNT;
                    var _delay = setup.ClusterNode.NodeDelay >= 0 ? setup.ClusterNode.NodeDelay : DiscoveryNode.DEFAULT_TRY_DELAY;
                    DiscoveryNode = new DiscoveryNode(this, setup.ClusterEndPoint.HostName, setup.ClusterEndPoint.Port, _tries, _delay);
                }
                else
                    DiscoveryNode = new DiscoveryNode(this, setup.ClusterEndPoint.HostName, setup.ClusterEndPoint.Port);
            }
            else
            {
                throw new ArgumentException("The provided endpoint does not support auto discovery");
            }
        }

        #endregion

        #region Members

        /// <summary>
        /// Gets a list of <see cref="T:IPEndPoint"/> each representing a Memcached server in the pool.
        /// </summary>
        public IList<EndPoint> Servers { get; }

        /// <summary>
        /// Gets the configuration of the socket pool.
        /// </summary>
        public ISocketPoolConfiguration SocketPool { get; }

        /// <summary>
        /// Gets the authentication settings.
        /// </summary>
        public IAuthenticationConfiguration Authentication { get; set; }

        /// <summary>
        /// Gets or sets the <see cref="T:Enyim.Caching.Memcached.IMemcachedKeyTransformer"/> which will be used to convert item keys for Memcached.
        /// </summary>
        public IMemcachedKeyTransformer KeyTransformer
        {
            get { return keyTransformer ?? (keyTransformer = new DefaultKeyTransformer()); }
            set { keyTransformer = value; }
        }

        /// <summary>
        /// Gets or sets the Type of the <see cref="T:Enyim.Caching.Memcached.IMemcachedNodeLocator"/> which will be used to assign items to Memcached nodes.
        /// </summary>
        /// <remarks>If both <see cref="M:NodeLocator"/> and  <see cref="M:NodeLocatorFactory"/> are assigned then the latter takes precedence.</remarks>
        public Type NodeLocator
        {
            get { return nodeLocator; }
            set
            {
                ConfigurationHelper.CheckForInterface(value, typeof(IMemcachedNodeLocator));
                nodeLocator = value;
            }
        }

        /// <summary>
        /// Gets or sets the NodeLocatorFactory instance which will be used to create a new IMemcachedNodeLocator instances.
        /// </summary>
        /// <remarks>If both <see cref="M:NodeLocator"/> and  <see cref="M:NodeLocatorFactory"/> are assigned then the latter takes precedence.</remarks>
        public IProviderFactory<IMemcachedNodeLocator> NodeLocatorFactory { get; set; }

        /// <summary>
        /// Gets or sets the <see cref="T:Enyim.Caching.Memcached.ITranscoder"/> which will be used serialize or deserialize items.
        /// </summary>
        public ITranscoder Transcoder
        {
            get { return transcoder ?? (transcoder = new DefaultTranscoder()); }
            set { transcoder = value; }
        }

        /// <summary>
        /// Gets or sets the <see cref="T:Enyim.Caching.Memcached.IPerformanceMonitor"/> instance which will be used monitor the performance of the client.
        /// </summary>
        //public IPerformanceMonitor PerformanceMonitor { get; set; }

        /// <summary>
        /// Gets or sets the type of the communication between client and server.
        /// </summary>
        public MemcachedProtocol Protocol { get; set; }

        #endregion

        #region [ interface                     ]

        IList<EndPoint> IMemcachedClientConfiguration.Servers
        {
            get { return Servers; }
        }

        ISocketPoolConfiguration IMemcachedClientConfiguration.SocketPool
        {
            get { return SocketPool; }
        }

        IAuthenticationConfiguration IMemcachedClientConfiguration.Authentication
        {
            get { return Authentication; }
        }

        IMemcachedKeyTransformer IMemcachedClientConfiguration.CreateKeyTransformer()
        {
            return KeyTransformer;
        }

        IMemcachedNodeLocator IMemcachedClientConfiguration.CreateNodeLocator()
        {
            var f = NodeLocatorFactory;
            if (f != null) return f.Create();

            return NodeLocator == null
                    ? new DefaultNodeLocator()
                    : (IMemcachedNodeLocator)FastActivator.Create(NodeLocator);
        }

        ITranscoder IMemcachedClientConfiguration.CreateTranscoder()
        {
            return Transcoder;
        }

        IServerPool IMemcachedClientConfiguration.CreatePool()
        {
            switch (Protocol)
            {
                case MemcachedProtocol.Text:
                    Pool = new AutoServerPool(this, new Enyim.Caching.Memcached.Protocol.Text.TextOperationFactory());
                    break;
                case MemcachedProtocol.Binary:
                    Pool = new AutoBinaryPool(this);
                    break;
                default:
                    throw new ArgumentOutOfRangeException("Unknown protocol: " + (int)Protocol);
            }

            return Pool;
        }

        //IPerformanceMonitor IMemcachedClientConfiguration.CreatePerformanceMonitor()
        //{
        //    return PerformanceMonitor;
        //}

        #endregion
    }
}
