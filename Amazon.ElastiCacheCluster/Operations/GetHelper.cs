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
using System;
using System.Globalization;

using Amazon.ElastiCacheCluster.Helpers;

using Enyim.Caching.Memcached;

namespace Amazon.ElastiCacheCluster.Operations
{
    internal static class GetHelper
    {
        private static readonly Enyim.Caching.ILog Log = Enyim.Caching.LogManager.GetLogger(typeof(GetHelper));

        public static void FinishCurrent(PooledSocket socket)
        {
            var response = TextSocketHelper.ReadResponse(socket);

            if (string.Compare(response, "END", StringComparison.Ordinal) != 0)
                throw new MemcachedClientException("No END was received.");
        }

        public static GetResponse ReadItem(PooledSocket socket)
        {
            var description = TextSocketHelper.ReadResponse(socket);

            if (string.Compare(description, "END", StringComparison.Ordinal) == 0)
                return null;

            if (description.Length < 6 || string.Compare(description, 0, "VALUE ", 0, 6, StringComparison.Ordinal) != 0)
                throw new MemcachedClientException("No VALUE response received.\r\n" + description);

            ulong cas = 0;
            var parts = description.Split(' ');

            // response is:
            // VALUE <key> <flags> <bytes> [<cas unique>]
            // 0     1     2       3       4
            //
            // cas only exists in 1.2.4+
            //
            if (parts.Length == 5)
            {
                if (!ulong.TryParse(parts[4], out cas))
                    throw new MemcachedClientException("Invalid CAS VALUE received.");

            }
            else if (parts.Length < 4)
            {
                throw new MemcachedClientException("Invalid VALUE response received: " + description);
            }

            var flags = ushort.Parse(parts[2], CultureInfo.InvariantCulture);
            var length = int.Parse(parts[3], CultureInfo.InvariantCulture);

            var allData = new byte[length];
            var eod = new byte[2];

            socket.Read(allData, 0, length);
            socket.Read(eod, 0, 2); // data is terminated by \r\n

            var retval = new GetResponse(parts[1], flags, cas, allData);

            if (Log.IsDebugEnabled)
                Log.DebugFormat("Received value. Data type: {0}, size: {1}.", retval.Item.Flags, retval.Item.Data.Count);

            return retval;
        }
    }

    #region [ T:GetResponse                  ]
    internal class GetResponse
    {
        private GetResponse() { }
        public GetResponse(string key, ushort flags, ulong casValue, byte[] data) : this(key, flags, casValue, data, 0, data.Length) { }

        public GetResponse(string key, ushort flags, ulong casValue, byte[] data, int offset, int count)
        {
            this.Key = key;
            this.CasValue = casValue;

            this.Item = new CacheItem(flags, new ArraySegment<byte>(data, offset, count));
        }

        public readonly string Key;
        public readonly ulong CasValue;
        public readonly CacheItem Item;
    }
    #endregion

}

#region [ License information          ]
/* ************************************************************
 * 
 *    Copyright (c) 2010 Attila Kisk�, enyim.com
 *    
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *    
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *    
 * ************************************************************/
#endregion
