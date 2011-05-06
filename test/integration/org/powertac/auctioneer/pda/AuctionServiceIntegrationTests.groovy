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
//import org.powertac.common.Product

import org.powertac.common.Timeslot
import org.powertac.common.enumerations.ProductType
import org.powertac.common.enumerations.BuySellIndicator
import org.powertac.common.enumerations.ModReasonCode
import grails.test.GrailsUnitTestCase
import org.powertac.common.Orderbook
import org.powertac.common.*
import org.codehaus.groovy.grails.plugins.orm.auditable.AuditLogEvent
import org.powertac.common.interfaces.BrokerProxy

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
    sampleProduct = ProductType.Future
    //assert (sampleProduct.save())
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
    assertEquals(buyShout.limitPrice, ob.bids.first().limitPrice)
    assertEquals(buyShout.quantity, ob.bids.first().quantity)

    assertEquals(sellShout.limitPrice, ob.asks.first().limitPrice)
    assertEquals(sellShout.quantity, ob.asks.first().quantity)

    assertEquals(1, ob.asks.size())
    assertEquals(1, ob.bids.size())


    Orderbook persistedOb = Orderbook.findByTimeslot(sampleTimeslot)

    assertEquals(buyShout.limitPrice, persistedOb.bids.first().limitPrice)
    assertEquals(buyShout.quantity, persistedOb.bids.first().quantity)

    assertEquals(sellShout.limitPrice, persistedOb.asks.first().limitPrice)
    assertEquals(sellShout.quantity, persistedOb.asks.first().quantity)

    assertEquals(1, persistedOb.asks.size())
    assertEquals(1, persistedOb.bids.size())

    println "Size Ob: ${Orderbook.findAllByTimeslot(sampleTimeslot).size()}"
    def trace = AuditLogEvent.findAllByClassNameAndPersistedObjectId(persistedOb.getClass().getName(), persistedOb.id)
    trace.each { println "Log ${it.className} ${it.persistedObjectId}, prop:${it.propertyName} was ${it.oldValue}, now ${it.newValue}" }

  }

  /** Incoming shout (bid) adds quantity to existing price level                     */
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
    OrderbookEntry bestBid = ob.bids.first()
    assertEquals(buyShout2.limitPrice, bestBid.limitPrice)
    assertEquals((buyShout.quantity + buyShout2.quantity), bestBid.quantity)
    assertEquals(1, ob.bids.size())

    OrderbookEntry bestAsk = ob.asks.first()
    assertEquals(sellShout.limitPrice, bestAsk.limitPrice)
    assertEquals(sellShout.quantity, bestAsk.quantity)
    assertEquals(1, ob.asks.size())

    Orderbook persistedOb = Orderbook.findByTimeslot(sampleTimeslot)

    OrderbookEntry bestPersistedBid = persistedOb.bids.first()
    assertEquals(buyShout2.limitPrice, bestPersistedBid.limitPrice)
    assertEquals((buyShout.quantity + buyShout2.quantity), bestPersistedBid.quantity)
    assertEquals(1, persistedOb.bids.size())

    OrderbookEntry bestPersistedAsk = persistedOb.asks.first()
    assertEquals(sellShout.limitPrice, bestPersistedAsk.limitPrice)
    assertEquals(sellShout.quantity, bestPersistedAsk.quantity)
    assertEquals(1, persistedOb.asks.size())
  }

  void testProcessOfIncomingShoutSequenceConcerningOrderbookUpdateAndShoutPersistence() {
    sellShout.limitPrice = 13.0
    sellShout.quantity = 10.0
    auctionService.processShout(sellShout)

    Orderbook persistedOb = Orderbook.findByTimeslot(sampleTimeslot)
    OrderbookEntry bestAsk = persistedOb.asks.first()
    assertNotNull(persistedOb)
    assertEquals(13.0, bestAsk.limitPrice)
    assertEquals(10.0, bestAsk.quantity)
    assertEquals(1, persistedOb.asks.size())
    assertEquals(0, persistedOb.bids.size())

    buyShout.limitPrice = 10.0
    buyShout.quantity = 10.0
    auctionService.processShout(buyShout)

    persistedOb = Orderbook.findByTimeslot(sampleTimeslot)
    bestAsk = persistedOb.asks.first()
    OrderbookEntry bestBid = persistedOb.bids.first()
    assertNotNull(persistedOb)
    assertEquals(13.0, bestAsk.limitPrice)
    assertEquals(10.0, bestAsk.quantity)
    assertEquals(1, persistedOb.asks.size())

    assertEquals(10.0, bestBid.limitPrice)
    assertEquals(10.0, bestBid.quantity)
    assertEquals(1, persistedOb.bids.size())


    Shout sellShout2 = new Shout(sellShout.properties)
    sellShout2.limitPrice = 12.0
    sellShout2.quantity = 20
    auctionService.processShout(sellShout2)

    persistedOb = Orderbook.findByTimeslot(sampleTimeslot)
    bestAsk = persistedOb.asks.first()
    bestBid = persistedOb.bids.first()
    def iter = persistedOb.asks.iterator()
    iter.next()
    OrderbookEntry secondBestAsk = iter.next()
    assertNotNull(persistedOb)
    assertEquals(12.0, bestAsk.limitPrice)
    assertEquals(20.0, bestAsk.quantity)
    assertEquals(13.0, secondBestAsk.limitPrice)
    assertEquals(10.0, secondBestAsk.quantity)
    assertEquals(2, persistedOb.asks.size())

    assertEquals(10.0, bestBid.limitPrice)
    assertEquals(10.0, bestBid.quantity)
    assertEquals(1, persistedOb.bids.size())

    Shout buyShout2 = new Shout(buyShout.properties)
    buyShout2.limitPrice = 10.0
    buyShout2.quantity = 30.0
    auctionService.processShout(buyShout2)

    persistedOb = Orderbook.findByTimeslot(sampleTimeslot)
    bestAsk = persistedOb.asks.first()
    bestBid = persistedOb.bids.first()
    iter = persistedOb.asks.iterator()
    iter.next()
    secondBestAsk = iter.next()

    assertNotNull(persistedOb)
    assertEquals(12.0, bestAsk.limitPrice)
    assertEquals(20.0, bestAsk.quantity)
    assertEquals(13.0, secondBestAsk.limitPrice)
    assertEquals(10.0, secondBestAsk.quantity)
    assertEquals(2, persistedOb.asks.size())

    assertEquals(10.0, bestBid.limitPrice)
    assertEquals(40.0, bestBid.quantity)
    assertEquals(1, persistedOb.bids.size())

    //validate Shouts
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
    // brokerProxy for AccountingService
    def messages = [:]
    def proxy =
    [sendMessage: {broker, message ->
      if (messages[broker] == null) {
        messages[broker] = []
      }
      messages[broker] << message
    },
            sendMessages: {broker, messageList ->
              if (messages[broker] == null) {
                messages[broker] = []
              }
              messageList.each { message ->
                messages[broker] << message
              }
            }] as BrokerProxy
    accountingService.brokerProxyService = proxy

    //init
    sellShout.limitPrice = 11.0
    sellShout.quantity = 20.0
    auctionService.processShout(sellShout)

    buyShout.limitPrice = 11.0
    buyShout.quantity = 10.0
    auctionService.processShout(buyShout)

    buyShout2.limitPrice = 2.0
    buyShout2.quantity = 10.0
    auctionService.processShout(buyShout2)

    //action
    auctionService.clearMarket()

    // Validate persisted obejcts
    assertEquals(3, Shout.list().size())

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

    Shout s5 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 2.0)
      eq('quantity', 10.0)
      eq('modReasonCode', ModReasonCode.DELETIONBYSYSTEM)
    }
    assertNotNull(s5)
    assertEquals(BuySellIndicator.BUY, s5.buySellIndicator)

    // Validate settlement
    accountingService.activate(timeService.currentTime, 3)
    MarketPosition mpBuyer = MarketPosition.findByBrokerAndTimeslot(sampleBuyer, sampleTimeslot)
    MarketPosition mpSeller = MarketPosition.findByBrokerAndTimeslot(sampleSeller, sampleTimeslot)

    assertEquals(10.0, mpBuyer.overallBalance)
    assertEquals(-10.0, mpSeller.overallBalance)

    //assertEquals(-110.0, mpBuyer.broker.cash.overallBalance)
    //assertEquals(110.0, mpSeller.broker.cash.overallBalance)

    Orderbook persistedOb = Orderbook.findByTimeslot(sampleTimeslot)
    def iter = persistedOb.bids.iterator()
    def bestBid = iter.next()
    def secondBestBid = iter.next()
    def bestAsk = persistedOb.asks.first()

    assertEquals(11.0, bestAsk.limitPrice)
    assertEquals(20.0, bestAsk.quantity)
    assertEquals(1, persistedOb.asks.size())

    assertEquals(11.0, bestBid.limitPrice)
    assertEquals(10.0, bestBid.quantity)
    assertEquals(2.0, secondBestBid.limitPrice)
    assertEquals(10.0, secondBestBid.quantity)
    assertEquals(2, persistedOb.bids.size())

    ClearedTrade persistedCt = ClearedTrade.findByTimeslot(sampleTimeslot)
    assertNotNull(persistedCt)
    assertEquals(11.0, persistedCt.executionPrice)
    assertEquals(10.0, persistedCt.executionQuantity)
  }

  /* Todo: Test market clearing and settlement in more complex situations*/
}
