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
using System.Globalization;
using System.Threading.Tasks;

using Amazon.ElastiCacheCluster.Helpers;

using Enyim.Caching.Memcached;
using Enyim.Caching.Memcached.Protocol;
using Enyim.Caching.Memcached.Results;
using Enyim.Caching.Memcached.Results.Extensions;

namespace Amazon.ElastiCacheCluster.Operations
{
    /// <summary>
    /// Used to get auto discovery information from ElastiCache endpoints version 1.4.14 or higher
    /// </summary>
    internal class ConfigGetOperation : SingleItemOperation, IGetOperation, IConfigOperation
    {
        /// <summary>
        /// Creates a config get for ElastiCache
        /// </summary>
        /// <param name="key"></param>
        public ConfigGetOperation(string key) : base(key) { }

        protected override IList<ArraySegment<byte>> GetBuffer()
        {
            var command = "config get " + Key + TextSocketHelper.CommandTerminator;

            return TextSocketHelper.GetCommandBuffer(command);
        }

        protected override IOperationResult ReadResponse(PooledSocket socket)
        {
            string description = TextSocketHelper.ReadResponse(socket);

            if (string.Compare(description, "END", StringComparison.Ordinal) == 0)
                return null;

            if (description.Length < 7 || string.Compare(description, 0, "CONFIG ", 0, 7, StringComparison.Ordinal) != 0)
                throw new MemcachedClientException("No CONFIG response received.\r\n" + description);

            string[] parts = description.Split(' ');

            /****** Format ********
             *
             * CONFIG <key> <flags> <bytes>
             * 0        1       2       3
             * 
             */

            ushort flags = ushort.Parse(parts[2], CultureInfo.InvariantCulture);
            int length = int.Parse(parts[3], CultureInfo.InvariantCulture);

            byte[] allNodes = new byte[length];
            byte[] eod = new byte[2];

            socket.Read(allNodes, 0, length);
            socket.Read(eod, 0, 2); // data is terminated by \r\n

            Result = new CacheItem(flags, new ArraySegment<byte>(allNodes, 0, length));
            ConfigResult = Result;

            string response = TextSocketHelper.ReadResponse(socket);

            if (string.Compare(response, "END", StringComparison.Ordinal) != 0)
                throw new MemcachedClientException("No END was received.");

            var result = new TextOperationResult();
            return result.Pass();
        }

        /// <summary>
        /// The CacheItem result of a "config get *key*" request
        /// </summary>
        public CacheItem Result { get; private set; }

        public CacheItem ConfigResult { get; set; }

        protected override bool ReadResponseAsync(PooledSocket socket, Action<bool> next)
        {
            throw new NotSupportedException();
        }

        protected override Task<IOperationResult> ReadResponseAsync(PooledSocket socket)
        {
            throw new NotImplementedException();
        }
    }
}
