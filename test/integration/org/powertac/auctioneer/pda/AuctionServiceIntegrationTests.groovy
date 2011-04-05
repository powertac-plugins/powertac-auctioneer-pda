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

import org.powertac.common.Shout
import org.joda.time.Instant
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.powertac.common.Broker
import org.powertac.common.Product
import org.powertac.common.Timeslot
import org.powertac.common.enumerations.ProductType
import org.powertac.common.enumerations.BuySellIndicator
import org.powertac.common.enumerations.ModReasonCode
import grails.test.GrailsUnitTestCase
import org.powertac.common.Orderbook
import org.powertac.common.*
import org.codehaus.groovy.grails.plugins.orm.auditable.AuditLogEvent

/**
 * Testing the auctionService
 *
 * @author Daniel Schnurr
 */

class AuctionServiceIntegrationTests extends GrailsUnitTestCase {

  Instant start
  def sampleTimeslot
  def sampleProduct
  def sampleSeller
  def sampleBuyer
  Shout buyShout
  Shout buyShout2
  Shout sellShout

  def auctionService
  def accountingService
  def timeService

  protected void setUp() {
    super.setUp()

    AuditLogEvent.list()*.delete()
    def now = new DateTime(2011, 1, 26, 12, 0, 0, 0, DateTimeZone.UTC).toInstant()
    timeService.setCurrentTime(now)
    sampleSeller = new Broker(username: "SampleSeller")
    assert (sampleSeller.save())
    sampleBuyer = new Broker(username: "SampleBuyer")
    assert (sampleBuyer.save())
    sampleProduct = new Product(productType: ProductType.Future)
    assert (sampleProduct.save())
    // set up some timeslots
    def ts = new Timeslot(serialNumber: 3,
            startInstant: new Instant(now.millis - TimeService.HOUR),
            endInstant: now, enabled: false)
    assert (ts.save())
    ts = new Timeslot(serialNumber: 4,
            startInstant: now,
            endInstant: new Instant(now.millis + TimeService.HOUR),
            enabled: false)
    assert (ts.save())
    sampleTimeslot = new Timeslot(serialNumber: 5,
            startInstant: new Instant(now.millis + TimeService.HOUR),
            endInstant: new Instant(now.millis + TimeService.HOUR * 2),
            enabled: true)
    assert (sampleTimeslot.validate())
    assert (sampleTimeslot.save())
    ts = new Timeslot(serialNumber: 6,
            startInstant: new Instant(now.millis + TimeService.HOUR * 2),
            endInstant: new Instant(now.millis + TimeService.HOUR * 3),
            enabled: true)
    assert (ts.save())



    buyShout = new Shout(broker: sampleBuyer, product: sampleProduct, timeslot: sampleTimeslot, buySellIndicator: BuySellIndicator.BUY)
    buyShout2 = new Shout(broker: sampleBuyer, product: sampleProduct, timeslot: sampleTimeslot, buySellIndicator: BuySellIndicator.BUY)
    sellShout = new Shout(broker: sampleSeller, product: sampleProduct, timeslot: sampleTimeslot, buySellIndicator: BuySellIndicator.SELL)
  }

  protected void tearDown() {
    super.tearDown()
  }

  void testIncomingSellShout() {
    sellShout.limitPrice = 10.0
    sellShout.quantity = 20.0
    auctionService.processShout(sellShout)

    assertEquals(1, Shout.list().size())
    Shout s1 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 10.0)
      eq('quantity', 20.0)
    }
    assertNotNull s1
    assertEquals(ModReasonCode.INSERT, s1.modReasonCode)
    assertNotNull s1.id
    assertNotNull s1.transactionId
  }

  void testIncomingBuyShout() {
    buyShout.limitPrice = 10.0
    buyShout.quantity = 20.0
    auctionService.processShout(buyShout)

    assertEquals(1, Shout.list().size())
    Shout s1 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 10.0)
      eq('quantity', 20.0)
    }
    assertNotNull s1
    assertEquals(ModReasonCode.INSERT, s1.modReasonCode)
    assertNotNull s1.id
    assertNotNull s1.transactionId
  }

  void testIncomingBuyAndSellShout() {
    sellShout.limitPrice = 10.0
    sellShout.quantity = 10.0
    auctionService.processShout(sellShout)

    buyShout.limitPrice = 12.0
    buyShout.quantity = 10.0
    auctionService.processShout(buyShout)

    assertEquals(2, Shout.list().size())

    Shout s1 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 10.0)
      eq('quantity', 10.0)
    }
    assertNotNull s1
    assertEquals(ModReasonCode.INSERT, s1.modReasonCode)
    assertNotNull s1.id
    assertNotNull s1.transactionId

    Shout b1 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 12.0)
      eq('quantity', 10.0)
    }
    assertNotNull b1
    assertEquals(ModReasonCode.INSERT, b1.modReasonCode)
    assertNotNull b1.id
    assertNotNull b1.transactionId
  }

  void testSimpleAskAndBidEntryInEmptyOrderbook() {
    /** init                */
    buyShout.quantity = 50
    buyShout.limitPrice = 11
    buyShout.transactionId = "tbd"
    assert buyShout.save()

    auctionService.updateOrderbook(buyShout)

    sellShout.quantity = 20
    sellShout.limitPrice = 13
    sellShout.transactionId = "tbd2"
    assert sellShout.save()

    //action
    Orderbook ob = auctionService.updateOrderbook(sellShout)

    //validate
    assertEquals(buyShout.limitPrice, ob.bid0)
    assertEquals(buyShout.quantity, ob.bidSize0)

    assertEquals(sellShout.limitPrice, ob.ask0)
    assertEquals(sellShout.quantity, ob.askSize0)

    assertNull(ob.bid1)
    assertEquals(0, ob.bidSize1)
    assertNull(ob.ask1)
    assertEquals(0, ob.askSize1)

    Orderbook persistedOb = Orderbook.findByTimeslot(sampleTimeslot)
    println "Size Ob: ${Orderbook.findAllByTimeslot(sampleTimeslot).size()}"
    def trace = AuditLogEvent.findAllByClassNameAndPersistedObjectId(persistedOb.getClass().getName(), persistedOb.id)
    trace.each { println "Log ${it.className} ${it.persistedObjectId}, prop:${it.propertyName} was ${it.oldValue}, now ${it.newValue}" }

  }

  /** Incoming shout (bid) adds quantity to existing price level                */
  void testAggregationUpdateOfEmptyOrderbook() {
    //init
    buyShout.quantity = 50
    buyShout.limitPrice = 11
    buyShout.transactionId = "tbd"
    assert buyShout.save()
    auctionService.updateOrderbook(buyShout)

    sellShout.quantity = 20
    sellShout.limitPrice = 13
    sellShout.transactionId = "tbd2"
    assert sellShout.save()
    auctionService.updateOrderbook(sellShout)

    //action
    buyShout2.quantity = 15
    buyShout2.limitPrice = 11
    buyShout2.transactionId = "tbd3"
    assert buyShout2.save()
    Orderbook ob = auctionService.updateOrderbook(buyShout2)

    //validate
    assertEquals(buyShout2.limitPrice, ob.bid0)
    assertEquals((buyShout.quantity + buyShout2.quantity), ob.bidSize0)

    assertNull(ob.bid1)
    assertEquals(0, ob.bidSize1)

    assertEquals(sellShout.limitPrice, ob.ask0)
    assertEquals(sellShout.quantity, ob.askSize0)

    assertNull(ob.ask1)
    assertEquals(0, ob.askSize1)
  }

  void testProcessOfIncomingShoutSequenceConcerningOrderbookUpdateAndShoutPersistence() {
    //init + action
    sellShout.limitPrice = 13.0
    sellShout.quantity = 10.0
    auctionService.processShout(sellShout)

    Orderbook firstPersistedOb = new Orderbook(Orderbook.findByProductAndTimeslot(sampleProduct, sampleTimeslot).properties)

    buyShout.limitPrice = 10.0
    buyShout.quantity = 10.0
    auctionService.processShout(buyShout)

    Orderbook secondPersistedOb = new Orderbook(Orderbook.findByProductAndTimeslot(sampleProduct, sampleTimeslot).properties)

    Shout sellShout2 = new Shout(sellShout.properties)
    sellShout2.limitPrice = 12.0
    sellShout2.quantity = 20
    auctionService.processShout(sellShout2)

    Orderbook thirdPersistedOb = new Orderbook(Orderbook.findByProductAndTimeslot(sampleProduct, sampleTimeslot).properties)

    Shout buyShout2 = new Shout(buyShout.properties)
    buyShout2.limitPrice = 10.0
    buyShout2.quantity = 30.0
    auctionService.processShout(buyShout2)

    Orderbook fourthPersistedOb = new Orderbook(Orderbook.findByProductAndTimeslot(sampleProduct, sampleTimeslot).properties)

    //validate
    assertEquals(4, Shout.list().size())
    List persistedShouts = []

    Shout s1 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 13.0)
      eq('quantity', 10.0)
    }
    assertNotNull s1
    persistedShouts << s1

    Shout s2 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 10.0)
      eq('quantity', 10.0)
    }
    assertNotNull s2
    persistedShouts << s2

    Shout s3 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 12.0)
      eq('quantity', 20.0)
    }
    assertNotNull s3
    persistedShouts << s3

    Shout s4 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 10.0)
      eq('quantity', 30.0)
    }
    assertNotNull s4
    persistedShouts << s4

    persistedShouts.each {shout ->
      assertEquals(ModReasonCode.INSERT, shout.modReasonCode)
      assertNotNull shout.transactionId
    }

    assertNotNull(firstPersistedOb)
    assertEquals(13.0, firstPersistedOb.ask0)
    assertEquals(10.0, firstPersistedOb.askSize0)
    assertNull(firstPersistedOb.bid0)
    assertEquals(0.0, firstPersistedOb.bidSize0)
    assertNull(firstPersistedOb.bid1)
    assertEquals(0.0, firstPersistedOb.bidSize1)
    assertNull(firstPersistedOb.ask1)
    assertEquals(0.0, firstPersistedOb.askSize1)

    assertNotNull(secondPersistedOb)
    assertEquals(13.0, secondPersistedOb.ask0)
    assertEquals(10.0, secondPersistedOb.askSize0)
    assertEquals(10.0, secondPersistedOb.bid0)
    assertEquals(10.0, secondPersistedOb.bidSize0)
    assertNotNull(secondPersistedOb)
    assertNull(secondPersistedOb.bid1)
    assertEquals(0.0, secondPersistedOb.bidSize1)
    assertNull(secondPersistedOb.ask1)
    assertEquals(0.0, secondPersistedOb.askSize1)

    assertNotNull(thirdPersistedOb)
    assertEquals(12.0, thirdPersistedOb.ask0)
    assertEquals(20.0, thirdPersistedOb.askSize0)
    assertEquals(10.0, thirdPersistedOb.bid0)
    assertEquals(10.0, thirdPersistedOb.bidSize0)
    assertNull(thirdPersistedOb.bid1)
    assertEquals(0.0, thirdPersistedOb.bidSize1)
    assertEquals(13.0, thirdPersistedOb.ask1)
    assertEquals(10.0, thirdPersistedOb.askSize1)
    assertNull(thirdPersistedOb.ask2)
    assertEquals(0.0, thirdPersistedOb.askSize2)

    assertNotNull(fourthPersistedOb)
    assertEquals(12.0, fourthPersistedOb.ask0)
    assertEquals(20.0, fourthPersistedOb.askSize0)
    assertEquals(10.0, fourthPersistedOb.bid0)
    assertEquals(40.0, fourthPersistedOb.bidSize0)
    assertNull(fourthPersistedOb.bid1)
    assertEquals(0.0, fourthPersistedOb.bidSize1)
    assertEquals(13.0, fourthPersistedOb.ask1)
    assertEquals(10.0, fourthPersistedOb.askSize1)
    assertNull(thirdPersistedOb.ask2)
    assertEquals(0.0, thirdPersistedOb.askSize2)
  }


  def testCompleteAllocationOfSingleBuyShout = {->
    //init
    buyShout.quantity = 50
    buyShout.limitPrice = 11
    buyShout.transactionId = "tbd"
    assert buyShout.save()

    BigDecimal aggregQuantityAllocated = 100
    Turnover turnover = new Turnover(price: 10, executableVolume: 200)
    String transactionId = "123"

    //action
    BigDecimal updatedAggregQuantityAllocated = auctionService.allocateSingleShout(buyShout, aggregQuantityAllocated, turnover, transactionId)

    //validate
    assertEquals(aggregQuantityAllocated + buyShout.quantity, updatedAggregQuantityAllocated)

    assertEquals(1, Shout.list().size())

    Shout persistedShout = (Shout) Shout.findByModReasonCode(ModReasonCode.EXECUTION)

    assertEquals(turnover.price, persistedShout.executionPrice)
    assertEquals(buyShout.quantity, persistedShout.executionQuantity)
    assertEquals(transactionId, persistedShout.transactionId)
    assertEquals("Matched by org.powertac.auctioneer.pda", persistedShout.comment)
  }

  void testSimpleUniformPriceCalculation() {
    //init
    sellShout.limitPrice = 11.0
    sellShout.quantity = 20.0
    auctionService.processShout(sellShout)

    buyShout.limitPrice = 11.0
    buyShout.quantity = 10.0
    auctionService.processShout(buyShout)

    //action
    def shouts = Shout.list()
    Turnover turnover = auctionService.calcUniformPrice(shouts)

    //validate
    assertEquals(11.0, turnover.price)
    assertEquals(10.0, turnover.getExecutableVolume())
    assertEquals(20.0, turnover.aggregatedQuantityAsk)
    assertEquals(10.0, turnover.aggregatedQuantityBid)
  }

  void testUniformPriceCalculationWithHigherMinimumAskQuantityAtLowestPrice() {
    //init
    sellShout.limitPrice = 11.0
    sellShout.quantity = 20.0
    auctionService.processShout(sellShout)

    Shout sellShout2 = new Shout(sellShout.properties)
    sellShout2.limitPrice = 13.0
    sellShout2.quantity = 10.0
    auctionService.processShout(sellShout2)

    buyShout.limitPrice = 13.0
    buyShout.quantity = 10.0
    auctionService.processShout(buyShout)

    Shout buyShout2 = new Shout(buyShout.properties)
    buyShout2.limitPrice = 11.0
    buyShout2.quantity = 70.0
    auctionService.processShout(buyShout2)

    //action
    def shouts = Shout.list()
    Turnover turnover = auctionService.calcUniformPrice(shouts)

    //validate
    assertEquals(11.0, turnover.price)
    assertEquals(20.0, turnover.executableVolume)
    assertEquals(20.0, turnover.aggregatedQuantityAsk)
    assertEquals(80.0, turnover.aggregatedQuantityBid)
  }

  void testUniformPriceCalculationWithHighestMinimumAskQuantityAndMinimalSurplus() {
    //init
    sellShout.limitPrice = 11.0
    sellShout.quantity = 20.0
    auctionService.processShout(sellShout)

    Shout sellShout2 = new Shout(sellShout.properties)
    sellShout2.limitPrice = 13.0
    sellShout2.quantity = 10.0
    auctionService.processShout(sellShout2)

    buyShout.limitPrice = 13.0
    buyShout.quantity = 10.0
    auctionService.processShout(buyShout)

    Shout buyShout2 = new Shout(buyShout.properties)
    buyShout2.limitPrice = 12.0
    buyShout2.quantity = 30.0
    auctionService.processShout(buyShout2)

    Shout buyShout3 = new Shout(buyShout.properties)
    buyShout3.limitPrice = 11.0
    buyShout3.quantity = 40.0
    auctionService.processShout(buyShout3)

    //action
    def shouts = Shout.list()
    Turnover turnover = auctionService.calcUniformPrice(shouts)

    //validate
    assertEquals(12.0, turnover.price)
    assertEquals(20.0, turnover.executableVolume)
    assertEquals(20.0, turnover.aggregatedQuantityAsk)
    assertEquals(40.0, turnover.aggregatedQuantityBid)
  }

  void testUniformPriceCalculationWithHighestMinimumBidQuantityAtMiddlePrice() {
    //init
    sellShout.limitPrice = 11.0
    sellShout.quantity = 20.0
    auctionService.processShout(sellShout)

    Shout sellShout2 = new Shout(sellShout.properties)
    sellShout2.limitPrice = 12.0
    sellShout2.quantity = 30.0
    auctionService.processShout(sellShout2)

    Shout sellShout3 = new Shout(sellShout.properties)
    sellShout3.limitPrice = 13.0
    sellShout3.quantity = 20.0
    auctionService.processShout(sellShout3)

    buyShout.limitPrice = 13.0
    buyShout.quantity = 10.0
    auctionService.processShout(buyShout)

    Shout buyShout2 = new Shout(buyShout.properties)
    buyShout2.limitPrice = 12.0
    buyShout2.quantity = 30.0
    auctionService.processShout(buyShout2)

    Shout buyShout3 = new Shout(buyShout.properties)
    buyShout3.limitPrice = 11.0
    buyShout3.quantity = 40.0
    auctionService.processShout(buyShout3)

    //action
    def shouts = Shout.list()
    Turnover turnover = auctionService.calcUniformPrice(shouts)

    //validate
    assertEquals(12.0, turnover.price)
    assertEquals(40.0, turnover.executableVolume)
    assertEquals(50.0, turnover.aggregatedQuantityAsk)
    assertEquals(40.0, turnover.aggregatedQuantityBid)
  }

  void testSimpleMarketClearing() {
    //init
    sellShout.limitPrice = 11.0
    sellShout.quantity = 20.0
    auctionService.processShout(sellShout)

    buyShout.limitPrice = 11.0
    buyShout.quantity = 10.0
    auctionService.processShout(buyShout)

    //action
    auctionService.clearMarket()

    // Validate persisted obejcts
    assertEquals(2, Shout.list().size())

    Shout s3 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 11.0)
      eq('quantity', 10.0)
      eq('modReasonCode', ModReasonCode.PARTIALEXECUTION)
    }
    assertNotNull(s3)
    //assert (s3.latest)
    assertEquals(BuySellIndicator.SELL, s3.buySellIndicator)
    assertEquals(10.0, s3.executionQuantity)
    assertEquals(11.0, s3.executionPrice)

    Shout s4 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 11.0)
      eq('quantity', 0.0)
      eq('modReasonCode', ModReasonCode.EXECUTION)
    }
    assertNotNull(s4)
    //assert (s4.latest)
    assertEquals(BuySellIndicator.BUY, s4.buySellIndicator)
    assertEquals(10.0, s4.executionQuantity)
    assertEquals(11.0, s4.executionPrice)

    // Validate settlement
    accountingService.activate(timeService.currentTime, 3)
    MarketPosition mpBuyer = MarketPosition.findByBrokerAndTimeslot(sampleBuyer, sampleTimeslot)
    MarketPosition mpSeller = MarketPosition.findByBrokerAndTimeslot(sampleSeller, sampleTimeslot)

    assertEquals(10.0, mpBuyer.overallBalance)
    assertEquals(-10.0, mpSeller.overallBalance)

    //assertEquals(-110.0, mpBuyer.broker.cash.overallBalance)
    //assertEquals(110.0, mpSeller.broker.cash.overallBalance)

    /* Todo Public information */
  }

  /* Todo: Test market clearing and settlement in more complex situations*/
}
