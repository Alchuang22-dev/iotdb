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

package org.apache.iotdb.library.dmatch.locomotif;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/** Searches the best representative segment induced by LoCo local paths. */
public final class CandidateFinder {

  private CandidateFinder() {}

  public static MotifSet findBest(
      List<LocalPath> paths,
      int n,
      int lMin,
      int lMax,
      double overlap,
      boolean[] rowMask,
      boolean[] colMask,
      boolean[] startMask,
      boolean[] endMask) {
    double bestFitness = 0.0d;
    Segment bestRepresentative = null;
    List<Segment> bestMembers = Collections.emptyList();

    int lastBegin = n - lMin;
    for (int representativeBegin = 0; representativeBegin <= lastBegin; representativeBegin++) {
      if (!startMask[representativeBegin]
          || colMask[representativeBegin]
          || LoCoMotifUtils.hasMasked(colMask, representativeBegin, representativeBegin + lMin)) {
        continue;
      }
      int maxEnd = Math.min(n, representativeBegin + lMax);
      for (int representativeEnd = representativeBegin + lMin;
          representativeEnd <= maxEnd;
          representativeEnd++) {
        if (colMask[representativeEnd - 1]) {
          break;
        }
        if (!endMask[representativeEnd - 1]) {
          continue;
        }
        Candidate candidate =
            evaluateCandidate(paths, n, representativeBegin, representativeEnd, overlap, rowMask);
        if (candidate != null && candidate.fitness > bestFitness) {
          bestFitness = candidate.fitness;
          bestRepresentative = new Segment(representativeBegin, representativeEnd);
          bestMembers = candidate.members;
        }
      }
    }

    if (bestRepresentative == null || bestFitness == 0.0d) {
      return null;
    }
    return new MotifSet(bestRepresentative, bestMembers, bestFitness);
  }

  private static Candidate evaluateCandidate(
      List<LocalPath> paths,
      int n,
      int representativeBegin,
      int representativeEnd,
      double overlap,
      boolean[] rowMask) {
    List<InducedSegment> inducedSegments = new ArrayList<>();
    for (LocalPath path : paths) {
      if (representativeBegin < path.getJ1() || path.getJl() < representativeEnd) {
        continue;
      }
      int beginPathIndex = path.findJ(representativeBegin);
      int endPathIndex = path.findJ(representativeEnd - 1);
      int begin = path.getRowAt(beginPathIndex);
      int end = path.getRowAt(endPathIndex) + 1;
      if (begin < 0 || end > rowMask.length || LoCoMotifUtils.hasMasked(rowMask, begin, end)) {
        continue;
      }
      inducedSegments.add(
          new InducedSegment(
              new Segment(begin, end),
              path.similaritySum(beginPathIndex, endPathIndex),
              endPathIndex - beginPathIndex + 1));
    }
    if (inducedSegments.size() < 2) {
      return null;
    }

    Collections.sort(
        inducedSegments,
        new Comparator<InducedSegment>() {
          @Override
          public int compare(InducedSegment o1, InducedSegment o2) {
            int beginCompare =
                Integer.compare(o1.segment.getBeginIndex(), o2.segment.getBeginIndex());
            if (beginCompare != 0) {
              return beginCompare;
            }
            return Integer.compare(o1.segment.getEndIndex(), o2.segment.getEndIndex());
          }
        });

    double score = 0.0d;
    double totalLength = 0.0d;
    double totalPathLength = 0.0d;
    double totalOverlap = 0.0d;
    int previousLength = 0;
    int previousEnd = 0;
    List<Segment> members = new ArrayList<>();

    for (int i = 0; i < inducedSegments.size(); i++) {
      InducedSegment inducedSegment = inducedSegments.get(i);
      Segment segment = inducedSegment.segment;
      int length = segment.length();
      if (i > 0) {
        int overlapLength = Math.max(0, previousEnd - segment.getBeginIndex());
        if (overlapLength > overlap * Math.min(length, previousLength)) {
          return null;
        }
        totalOverlap += overlapLength;
      }
      totalLength += length;
      totalPathLength += inducedSegment.pathLength;
      score += inducedSegment.score;
      previousLength = length;
      previousEnd = segment.getEndIndex();
      members.add(segment);
    }

    double representativeLength = representativeEnd - representativeBegin;
    double normalizedScore = (score - representativeLength) / totalPathLength;
    double normalizedCoverage = (totalLength - totalOverlap - representativeLength) / n;
    if (normalizedScore <= 0.0d || normalizedCoverage <= 0.0d) {
      return null;
    }
    double fitness =
        2.0d * (normalizedCoverage * normalizedScore) / (normalizedCoverage + normalizedScore);
    return new Candidate(members, fitness);
  }

  private static class Candidate {
    private final List<Segment> members;
    private final double fitness;

    private Candidate(List<Segment> members, double fitness) {
      this.members = members;
      this.fitness = fitness;
    }
  }

  private static class InducedSegment {
    private final Segment segment;
    private final double score;
    private final int pathLength;

    private InducedSegment(Segment segment, double score, int pathLength) {
      this.segment = segment;
      this.score = score;
      this.pathLength = pathLength;
    }
  }
}
