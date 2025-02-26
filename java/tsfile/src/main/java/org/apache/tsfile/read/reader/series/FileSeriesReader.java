/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.read.reader.series;

import org.apache.tsfile.file.metadata.AbstractAlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.controller.IChunkLoader;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.read.reader.chunk.TableChunkReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Series reader is used to query one series of one TsFile, and this reader has a filter operating
 * on the same series.
 */
public class FileSeriesReader extends AbstractFileSeriesReader {

  public FileSeriesReader(
      IChunkLoader chunkLoader, List<IChunkMetadata> chunkMetadataList, Filter filter) {
    super(chunkLoader, chunkMetadataList, filter);
  }

  public FileSeriesReader(
      IChunkLoader chunkLoader,
      List<IChunkMetadata> chunkMetadataList,
      Filter filter,
      boolean ignoreAllNullRows) {
    super(chunkLoader, chunkMetadataList, filter, ignoreAllNullRows);
  }

  @Override
  protected void initChunkReader(IChunkMetadata chunkMetaData) throws IOException {
    currentChunkMeasurementNames.clear();
    if (chunkMetaData instanceof ChunkMetadata) {
      Chunk chunk = chunkLoader.loadChunk((ChunkMetadata) chunkMetaData);
      this.chunkReader = new ChunkReader(chunk, filter);
      currentChunkMeasurementNames.add(chunkMetaData.getMeasurementUid());
    } else {
      AbstractAlignedChunkMetadata alignedChunkMetadata =
          (AbstractAlignedChunkMetadata) chunkMetaData;
      Chunk timeChunk =
          chunkLoader.loadChunk((ChunkMetadata) (alignedChunkMetadata.getTimeChunkMetadata()));
      List<Chunk> valueChunkList = new ArrayList<>();
      for (IChunkMetadata metadata : alignedChunkMetadata.getValueChunkMetadataList()) {
        if (metadata != null) {
          valueChunkList.add(chunkLoader.loadChunk((ChunkMetadata) metadata));
          currentChunkMeasurementNames.add(metadata.getMeasurementUid());
          continue;
        }
        valueChunkList.add(null);
        currentChunkMeasurementNames.add(null);
      }
      if (ignoreAllNullRows) {
        this.chunkReader = new AlignedChunkReader(timeChunk, valueChunkList, filter);
      } else {
        this.chunkReader = new TableChunkReader(timeChunk, valueChunkList, filter);
      }
    }
  }

  @Override
  protected boolean chunkCanSkip(IChunkMetadata chunkMetaData) {
    return filter != null && filter.canSkip(chunkMetaData);
  }
}
