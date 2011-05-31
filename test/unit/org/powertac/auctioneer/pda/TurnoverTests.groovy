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

import org.apache.commons.lang.builder.CompareToBuilder

class TurnoverTests extends GroovyTestCase {

  protected void setUp() {
    super.setUp()
  }

  protected void tearDown() {
    super.tearDown()
  }

  void testEquality() {
    Turnover t1 = new Turnover(price: 1.0, aggregatedQuantityAsk: 20.0, aggregatedQuantityBid: 25.0)
    Turnover t2 = new Turnover(price: 1.0, aggregatedQuantityAsk: 20.0, aggregatedQuantityBid: 25.0)
    assertEquals(t1, t2)
    t2.price = 2.0
    assertEquals(t1, t2)
    t2.aggregatedQuantityAsk = 1.0
    assertNotSame(t1, t2)
    t2.aggregatedQuantityAsk = 20.0
    assertEquals(t1, t2)
    t2.aggregatedQuantityBid = 1.0
    assertNotSame(t1, t2)
  }

  void testGetExecutableVolume() {
    Turnover t1 = new Turnover(price: 1.0, aggregatedQuantityAsk: 20.0, aggregatedQuantityBid: 25.0)
    assertEquals(20.0, t1.getExecutableVolume())
    t1.aggregatedQuantityBid = 15.0
    assertEquals(15.0, t1.getExecutableVolume())
    t1.aggregatedQuantityBid = 0.0
    assertEquals(0, t1.getExecutableVolume())
    t1.aggregatedQuantityBid = -50.0
    assertEquals(0, t1.getExecutableVolume())
  }

  void testSorting() {
    Turnover t1 = new Turnover(price: 2.0, aggregatedQuantityAsk: 0.0, aggregatedQuantityBid: 20.0)
    Turnover t2 = new Turnover(price: 11.0, aggregatedQuantityAsk: 20.0, aggregatedQuantityBid: 10.0)
    Turnover t3 = new Turnover(price: 11.0, aggregatedQuantityAsk: 20.0, aggregatedQuantityBid: 25.0)

    def list = [t1, t2].sort()
    def list2 = [t2, t3].sort()

    assertEquals(t2, list.last())
    assertEquals(t3, list2.last())
    //assertEquals(0, t1.compareTo(t2))

  }
}
