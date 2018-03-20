/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.segment.spi.persistence;

public interface SegmentArchiveEntry {

    /**
     * Return the most significant bits of the identifier of this entry.
     *
     * @return the most significant bits of the identifier of this entry.
     */
    long getMsb();

    /**
     * Return the least significant bits of the identifier of this entry.
     *
     * @return the least significant bits of the identifier of this entry.
     */
    long getLsb();

    /**
     * Return the length of this entry in the archive.
     *
     * @return the length of this entry in the archive.
     */
    int getLength();

    /**
     * Return the generation of this entry.
     *
     * @return the generation of this entry.
     */
    int getGeneration();

    /**
     * Return the full generation of this entry.
     *
     * @return the full generation of this entry.
     */
    int getFullGeneration();

    /**
     * Return {@code true} if this entry was generated by a compaction operation.
     *
     * @return {@code true} if this entry was generated by a compaction operation.
     */
    boolean isCompacted();

}
