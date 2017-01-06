/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Portions copyright 2010 Attila Kisk�, enyim.com. Please see LICENSE.txt
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

using System.Threading.Tasks;

using Enyim.Caching.Memcached;
using Enyim.Caching.Memcached.Protocol;
using Enyim.Caching.Memcached.Results;
using Enyim.Caching.Memcached.Results.Extensions;

using Amazon.ElastiCacheCluster.Helpers;

namespace Amazon.ElastiCacheCluster.Operations
{
    internal class GetOperation : SingleItemOperation, IGetOperation, IConfigOperation
    {
        private CacheItem result;

        internal GetOperation(string key) : base(key) { }

        protected override System.Collections.Generic.IList<System.ArraySegment<byte>> GetBuffer()
        {
            var command = "gets " + Key + TextSocketHelper.CommandTerminator;

            return TextSocketHelper.GetCommandBuffer(command);
        }

        protected override IOperationResult ReadResponse(PooledSocket socket)
        {
            GetResponse r = GetHelper.ReadItem(socket);
            var result = new TextOperationResult();

            if (r == null) return result.Fail("Failed to read response");

            this.result = r.Item;
            ConfigResult = r.Item;

            Cas = r.CasValue;

            GetHelper.FinishCurrent(socket);

            return result.Pass();
        }

        CacheItem IGetOperation.Result => result;

        public CacheItem ConfigResult { get; set; }

        protected override bool ReadResponseAsync(PooledSocket socket, System.Action<bool> next)
        {
            throw new System.NotSupportedException();
        }

        protected override Task<IOperationResult> ReadResponseAsync(PooledSocket socket)
        {
            throw new System.NotImplementedException();
        }
    }
}