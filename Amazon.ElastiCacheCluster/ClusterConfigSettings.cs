﻿/*
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

using Amazon.ElastiCacheCluster.Factories;

using Enyim.Caching.Configuration;
using Enyim.Caching.Memcached;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;

namespace Amazon.ElastiCacheCluster
{
    /// <summary>
    /// A config settings object used to configure the client config
    /// </summary>
    public class ClusterConfigSettings : ConfigurationSection
    {
        /// <summary>
        /// An object that produces nodes for the Discovery Node, mainly used for testing
        /// </summary>
        public IConfigNodeFactory NodeFactory { get; set; }

        #region Constructors

        /// <summary>
        /// For config manager
        /// </summary>
        public ClusterConfigSettings()
            : base(
                new ConfigurationRoot(new List<IConfigurationProvider>
                {
                    new JsonConfigurationProvider(new JsonConfigurationSource())
                }),
                $"{Directory.GetCurrentDirectory()}/appsettings.json")
        {
        }

        /// <summary>
        /// Used to initialize a setup with a host and port
        /// </summary>
        /// <param name="hostname">Cluster hostname</param>
        /// <param name="port">Cluster port</param>
        public ClusterConfigSettings(string hostname, int port)
            : base(
                new ConfigurationRoot(new List<IConfigurationProvider>
                {
                    new JsonConfigurationProvider(new JsonConfigurationSource())
                }),
                $"{Directory.GetCurrentDirectory()}/appsettings.json")
        {
            if (string.IsNullOrEmpty(hostname))
                throw new ArgumentNullException("hostname");
            if (port <= 0)
                throw new ArgumentException("Port cannot be less than or equal to zero");

            ClusterEndPoint.HostName = hostname;
            ClusterEndPoint.Port = port;
        }

        #endregion

        #region Config Settings

        /// <summary>
        /// Class containing information about the cluster host and port
        /// </summary>
        public Endpoint ClusterEndPoint { get; set; }

        /// <summary>
        /// Class containing information about the node configuration
        /// </summary>
        public NodeSettings ClusterNode { get; set; }

        /// <summary>
        /// Class containing information about the poller configuration
        /// </summary>
        public PollerSettings ClusterPoller { get; set; }

        /// <summary>
        /// Endpoint that contains the hostname and port for auto discovery
        /// </summary>
        public class Endpoint
        {
            /// <summary>
            /// The hostname of the cluster containing ".cfg."
            /// </summary>
            public string HostName { get; set; }

            /// <summary>
            /// The port of the endpoint
            /// </summary>
            public int Port { get; set; }
        }

        /// <summary>
        /// Settings used for the discovery node
        /// </summary>
        public class NodeSettings
        {
            public NodeSettings()
            {
                NodeTries = -1;
                NodeDelay = -1;
            }

            /// <summary>
            /// How many tries the node should use to get a config
            /// </summary>
            public int NodeTries { get; set; }

            /// <summary>
            /// The delay between tries for the config in miliseconds
            /// </summary>
            public int NodeDelay { get; set; }
        }

        /// <summary>
        /// Settins used for the configuration poller
        /// </summary>
        public class PollerSettings
        {
            public PollerSettings()
            {
                IntervalDelay = -1;
            }

            /// <summary>
            /// The delay between polls in miliseconds
            /// </summary>
            public int IntervalDelay { get; set; }
        }

        #endregion

        #region MemcachedConfig

        /// <summary>
        /// Gets or sets the type of the communication between client and server.
        /// </summary>
        public MemcachedProtocol Protocol { get; set; } = MemcachedProtocol.Binary;

        /// <summary>
        /// Gets or sets the configuration of the socket pool.
        /// </summary>
        public SocketPoolConfiguration SocketPool { get; set; } = new SocketPoolConfiguration();

        /// <summary>
        /// Gets or sets the configuration of the authenticator.
        /// </summary>
        public AuthenticationConfiguration Authentication { get; set; }

        /// <summary>
        /// Gets or sets the <see cref="T:Enyim.Caching.Memcached.IMemcachedKeyTransformer"/> which will be used to convert item keys for Memcached.
        /// </summary>
        public IMemcachedKeyTransformer KeyTransformer { get; set; }

        /// <summary>
        /// Gets or sets the <see cref="T:Enyim.Caching.Memcached.IMemcachedNodeLocator"/> which will be used to assign items to Memcached nodes.
        /// </summary>
        public IMemcachedNodeLocator NodeLocator { get; set; }

        /// <summary>
        /// Gets or sets the <see cref="T:Enyim.Caching.Memcached.ITranscoder"/> which will be used serialzie or deserialize items.
        /// </summary>
        public ITranscoder Transcoder { get; set; }

        #endregion

    }
}
