/*
 * Copyright (c) <current-year> by the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.powertac.auctioneer.pda

import org.apache.commons.lang.builder.EqualsBuilder
import org.apache.commons.lang.builder.CompareToBuilder
import org.apache.commons.lang.builder.HashCodeBuilder

/**
 * This class is used to store information on potentially tradeable volume under different uniform (market) prices
 * it implements Comparable and thus can be used in a SortedList. In Effect Turnover instances with the highest
 * tradeable volume (and if tradeable volume is equal among two different instances the instance with the lowest surplus)
 * are be default
 *
 * @author Carsten Block, Daniel Schnurr
 */

class Turnover implements Comparable {

  BigDecimal price
  BigDecimal aggregatedQuantityAsk
  BigDecimal aggregatedQuantityBid


  private void setExecutableVolume(BigDecimal executableVolume) {
    //do nothing, method just prevents generation of a public setter
  }

  public BigDecimal getExecutableVolume() {
    return Math.min(Math.max(0.0, aggregatedQuantityAsk), Math.max(0.0, aggregatedQuantityBid))
  }

  private void setSurplus(BigDecimal surplus) {
    //do nothing, method just prevents generation of a public setter
  }

  public BigDecimal getSurplus() {
    return Math.max(Math.max(0.0, aggregatedQuantityAsk), Math.max(0.0, aggregatedQuantityBid)) - getExecutableVolume()
  }

  public int hashCode() {
    return new HashCodeBuilder(17, 37).
            append(this.executableVolume).
            append(this.surplus).
            toHashCode();
  }

  public boolean equals(Object o) {
    if (!o instanceof Turnover) return false
    Turnover other = (Turnover) o
    return new EqualsBuilder().
            append(other?.executableVolume, this?.executableVolume).
            append(this?.surplus, other?.surplus).
            isEquals()
  }

  public int compareTo(Object o) {
    if (!o instanceof Turnover) return 1
    Turnover other = (Turnover) o
    return new CompareToBuilder().
            append(other?.executableVolume, this?.executableVolume).
            append(this?.surplus, other?.surplus).
            toComparison();
  }


}
