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
import com.sun.org.apache.xpath.internal.operations.Mod

/**
 * Testing the auctionService
 *
 * @author Daniel Schnurr
 */

class AuctionServiceIntegrationTests extends GrailsUnitTestCase {

  Instant start
  def sampleTimeslot
  def futureTimeslot
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
    futureTimeslot = new Timeslot(serialNumber: 6,
            startInstant: new Instant(now.millis + TimeService.HOUR * 2),
            endInstant: new Instant(now.millis + TimeService.HOUR * 3),
            enabled: true)
    assert (futureTimeslot.save())
    ts = new Timeslot(serialNumber: 7,
            startInstant: new Instant(now.millis + TimeService.HOUR * 3),
            endInstant: new Instant(now.millis + TimeService.HOUR * 4),
            enabled: false)
    assert (ts.save())



    buyShout = new Shout(broker: sampleBuyer, product: sampleProduct, timeslot: sampleTimeslot, buySellIndicator: BuySellIndicator.BUY)
    buyShout2 = new Shout(broker: sampleBuyer, product: sampleProduct, timeslot: sampleTimeslot, buySellIndicator: BuySellIndicator.BUY)
    sellShout = new Shout(broker: sampleSeller, product: sampleProduct, timeslot: sampleTimeslot, buySellIndicator: BuySellIndicator.SELL)
  }

  protected void tearDown() 
  {
    super.tearDown()
  }

  void simulateTransition() 
  {
    /** Simulate transition to next round: enable/disable timeslots  */
    Timeslot enabledTs = Timeslot.findBySerialNumber(7)

    sampleTimeslot.enabled = false
    assert (sampleTimeslot.save())

    enabledTs.enabled = true
    assert (enabledTs.save())
  }

  void testIncomingSellShout()
  {
    Shout sellShoutA = new Shout(sellShout.properties)
    sellShoutA.limitPrice = 10.0
    sellShoutA.quantity = 20.0
    auctionService.processShout(sellShoutA)

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

  void testIncomingBuyShout() 
  {
    Shout bs = new Shout(buyShout.properties)
    bs.limitPrice = 10.0
    bs.quantity = 20.0
    auctionService.processShout(bs)

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

  void testIncomingBuyAndSellShout() 
  {
    Shout ss1 = new Shout(sellShout.properties)
    ss1.limitPrice = 10.0
    ss1.quantity = 10.0
    auctionService.processShout(ss1)

    Shout bs1 = new Shout(buyShout.properties)
    bs1.limitPrice = 12.0
    bs1.quantity = 10.0
    auctionService.processShout(bs1)

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


  void testSimpleAskAndBidEntryInEmptyOrderbook() 
  {
    Shout bs1 = new Shout(buyShout.properties)
    bs1.quantity = 50
    bs1.limitPrice = 11
    bs1.transactionId = "tbd"
    assert bs1.save()

    auctionService.updateOrderbook(bs1)

    Shout ss1 = new Shout(sellShout.properties)
    ss1.quantity = 20
    ss1.limitPrice = 13
    ss1.transactionId = "tbd2"
    assert ss1.save()

    //action
    Orderbook ob = auctionService.updateOrderbook(ss1)

    //validate
    assertEquals(bs1.limitPrice, ob.bids.first().limitPrice)
    assertEquals(bs1.quantity, ob.bids.first().quantity)

    assertEquals(ss1.limitPrice, ob.asks.first().limitPrice)
    assertEquals(ss1.quantity, ob.asks.first().quantity)

    assertEquals(1, ob.asks.size())
    assertEquals(1, ob.bids.size())


    Orderbook persistedOb = Orderbook.findByTimeslot(sampleTimeslot)

    assertEquals(bs1.limitPrice, persistedOb.bids.first().limitPrice)
    assertEquals(bs1.quantity, persistedOb.bids.first().quantity)

    assertEquals(ss1.limitPrice, persistedOb.asks.first().limitPrice)
    assertEquals(ss1.quantity, persistedOb.asks.first().quantity)

    assertEquals(1, persistedOb.asks.size())
    assertEquals(1, persistedOb.bids.size())

    println "Size Ob: ${Orderbook.findAllByTimeslot(sampleTimeslot).size()}"
    def trace = AuditLogEvent.findAllByClassNameAndPersistedObjectId(persistedOb.getClass().getName(), persistedOb.id)
    trace.each { println "Log ${it.className} ${it.persistedObjectId}, prop:${it.propertyName} was ${it.oldValue}, now ${it.newValue}" }
  }

  void testSimpleAskEntryInEmptyOrderbook() 
  {
    Shout ss1 = new Shout(sellShout.properties)
    ss1.quantity = 20
    ss1.limitPrice = 13
    ss1.transactionId = "123"
    assert ss1.save()

    //action
    Orderbook ob = auctionService.updateOrderbook(ss1)

    //validate
    assertEquals(13, ob.asks.first().limitPrice)
    assertEquals(20, ob.asks.first().quantity)

    assertEquals(1, ob.asks.size())
    assertEquals(0, ob.bids.size())


    Orderbook persistedOb = Orderbook.findByTimeslot(sampleTimeslot)
    assertEquals(13, persistedOb.asks.first().limitPrice)
    assertEquals(20, persistedOb.asks.first().quantity)

    assertEquals(1, persistedOb.asks.size())
    assertEquals(0, persistedOb.bids.size())
  }

  void testSimpleBidEntryInEmptyOrderbook() 
  {
    Shout bs1 = new Shout(buyShout.properties)
    bs1.quantity = 50
    bs1.limitPrice = 11
    bs1.transactionId = "tbd"
    assert bs1.save()

    //action
    Orderbook ob = auctionService.updateOrderbook(bs1)

    //validate
    assertEquals(11, ob.bids.first().limitPrice)
    assertEquals(50, ob.bids.first().quantity)

    assertEquals(0, ob.asks.size())
    assertEquals(1, ob.bids.size())


    Orderbook persistedOb = Orderbook.findByTimeslot(sampleTimeslot)

    assertEquals(11, persistedOb.bids.first().limitPrice)
    assertEquals(50, persistedOb.bids.first().quantity)

    assertEquals(0, persistedOb.asks.size())
    assertEquals(1, persistedOb.bids.size())
  }

  /** Incoming shout (bid) adds quantity to existing price level                      */
  void testAggregationUpdateOfEmptyOrderbook() {
    //init
    Shout bs1 = new Shout(buyShout.properties)
    bs1.quantity = 50
    bs1.limitPrice = 11
    bs1.transactionId = "tbd"
    assert bs1.save()
    auctionService.updateOrderbook(bs1)

    Shout ss1 = new Shout(sellShout.properties)
    ss1.quantity = 20
    ss1.limitPrice = 13
    ss1.transactionId = "tbd2"
    assert ss1.save()
    auctionService.updateOrderbook(ss1)

    //action
    Shout bs2 = new Shout(buyShout2.properties)
    bs2.quantity = 15
    bs2.limitPrice = 11
    bs2.transactionId = "tbd3"
    assert bs2.save()
    Orderbook ob = auctionService.updateOrderbook(bs2)

    //validate
    OrderbookBid bestBid = ob.bids.first()
    assertEquals(bs2.limitPrice, bestBid.limitPrice)
    assertEquals((bs1.quantity + bs2.quantity), bestBid.quantity)
    assertEquals(1, ob.bids.size())

    OrderbookAsk bestAsk = ob.asks.first()
    assertEquals(ss1.limitPrice, bestAsk.limitPrice)
    assertEquals(ss1.quantity, bestAsk.quantity)
    assertEquals(1, ob.asks.size())

    Orderbook persistedOb = Orderbook.findByTimeslot(sampleTimeslot)

    OrderbookBid bestPersistedBid = persistedOb.bids.first()
    assertEquals(bs2.limitPrice, bestPersistedBid.limitPrice)
    assertEquals((bs1.quantity + bs2.quantity), bestPersistedBid.quantity)
    assertEquals(1, persistedOb.bids.size())

    OrderbookAsk bestPersistedAsk = persistedOb.asks.first()
    assertEquals(ss1.limitPrice, bestPersistedAsk.limitPrice)
    assertEquals(ss1.quantity, bestPersistedAsk.quantity)
    assertEquals(1, persistedOb.asks.size())
  }

  void testProcessOfIncomingShoutSequenceConcerningOrderbookUpdateAndShoutPersistence() 
  {
    Shout ss1 = new Shout(sellShout.properties)
    ss1.limitPrice = 13.0
    ss1.quantity = 10.0
    auctionService.processShout(ss1)

    Orderbook persistedOb = Orderbook.findByTimeslot(sampleTimeslot)
    OrderbookAsk bestAsk = persistedOb.asks.first()
    assertNotNull(persistedOb)
    assertEquals(13.0, bestAsk.limitPrice)
    assertEquals(10.0, bestAsk.quantity)
    assertEquals(1, persistedOb.asks.size())
    assertEquals(0, persistedOb.bids.size())

    Shout bs1 = new Shout(buyShout.properties)
    bs1.limitPrice = 10.0
    bs1.quantity = 10.0
    auctionService.processShout(bs1)

    persistedOb = Orderbook.findByTimeslot(sampleTimeslot)
    bestAsk = persistedOb.asks.first()
    OrderbookBid bestBid = persistedOb.bids.first()
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
    OrderbookAsk secondBestAsk = iter.next()
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


  void testCompleteAllocationOfSingleBuyShout() {
    //init
    buyShout.quantity = 50
    buyShout.limitPrice = 11
    buyShout.transactionId = "tbd"
    assert buyShout.save()

    BigDecimal aggregQuantityAllocated = 100
    Turnover turnover = new Turnover(price: 10, aggregatedQuantityAsk: 200, aggregatedQuantityBid: 240)
    String transactionId = "123"

    //action
    BigDecimal updatedAggregQuantityAllocated = auctionService.allocateSingleShout(buyShout, aggregQuantityAllocated, turnover, transactionId)

    //validate
    assertEquals(150, updatedAggregQuantityAllocated)

    assertEquals(1, Shout.list().size())

    Shout persistedShout = (Shout) Shout.findByModReasonCode(ModReasonCode.EXECUTION)

    assertEquals(turnover.price, persistedShout.executionPrice)
    assertEquals(50, persistedShout.executionQuantity)
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
    Shout sellShout1 = new Shout(sellShout.properties)
    sellShout1.limitPrice = 11.0
    sellShout1.quantity = 20.0
    auctionService.processShout(sellShout1)

    Shout sellShout2 = new Shout(sellShout.properties)
    sellShout2.limitPrice = 13.0
    sellShout2.quantity = 10.0
    auctionService.processShout(sellShout2)

    Shout buyShout1 = new Shout(buyShout.properties)
    buyShout1.limitPrice = 13.0
    buyShout1.quantity = 10.0
    auctionService.processShout(buyShout1)

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

  void testUniformPriceCalculationWithHighestMinimumAskQuantityAndMidpointPrice() 
  {
    //init
    Shout sellShout1 = new Shout(sellShout.properties)
    sellShout1.limitPrice = 11.0
    sellShout1.quantity = 20.0
    auctionService.processShout(sellShout1)

    Shout sellShout2 = new Shout(sellShout.properties)
    sellShout2.limitPrice = 13.0
    sellShout2.quantity = 10.0
    auctionService.processShout(sellShout2)

    Shout buyShout1 = new Shout(buyShout.properties)
    buyShout1.limitPrice = 13.0
    buyShout1.quantity = 10.0
    auctionService.processShout(buyShout1)

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
    assertEquals(11.5, turnover.price)
    assertEquals(20.0, turnover.executableVolume)
    assertEquals(20.0, turnover.aggregatedQuantityAsk)
    assertEquals(40.0, turnover.aggregatedQuantityBid)
  }

  void testUniformPriceCalculationWithHighestMinimumBidQuantityAtMiddlePrice() 
  {
    //init
    Shout sellShout1 = new Shout(sellShout.properties)
    sellShout1.limitPrice = 11.0
    sellShout1.quantity = 20.0
    auctionService.processShout(sellShout1)

    Shout sellShout2 = new Shout(sellShout.properties)
    sellShout2.limitPrice = 12.0
    sellShout2.quantity = 30.0
    auctionService.processShout(sellShout2)

    Shout sellShout3 = new Shout(sellShout.properties)
    sellShout3.limitPrice = 13.0
    sellShout3.quantity = 20.0
    auctionService.processShout(sellShout3)

    Shout buyShout1 = new Shout(buyShout.properties)
    buyShout1.limitPrice = 13.0
    buyShout1.quantity = 10.0
    auctionService.processShout(buyShout1)

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

  void testUniformPriceCalculationWithEqualExecutionQuantities() 
  {
    //init
    Shout sellShout1 = new Shout(sellShout.properties)
    sellShout1.limitPrice = 11.0
    sellShout1.quantity = 40.0
    auctionService.processShout(sellShout1)

    Shout sellShout2 = new Shout(sellShout.properties)
    sellShout2.limitPrice = 12.0
    sellShout2.quantity = 10.0
    auctionService.processShout(sellShout2)

    Shout sellShout3 = new Shout(sellShout.properties)
    sellShout3.limitPrice = 13.0
    sellShout3.quantity = 20.0
    auctionService.processShout(sellShout3)

    Shout buyShout1 = new Shout(buyShout.properties)
    buyShout1.limitPrice = 13.0
    buyShout1.quantity = 10.0
    auctionService.processShout(buyShout1)

    Shout buyShout2 = new Shout(buyShout.properties)
    buyShout2.limitPrice = 12.0
    buyShout2.quantity = 30.0
    auctionService.processShout(buyShout2)

    Shout buyShout3 = new Shout(buyShout.properties)
    buyShout3.limitPrice = 11.0
    buyShout3.quantity = 10.0
    auctionService.processShout(buyShout3)

    //action
    def shouts = Shout.list()
    Turnover turnover = auctionService.calcUniformPrice(shouts)

    //validate
    assertEquals(11.5, turnover.price)
    assertEquals(40.0, turnover.executableVolume)
    assertEquals(40.0, turnover.aggregatedQuantityAsk)
    assertEquals(40.0, turnover.aggregatedQuantityBid)
  }

  void testSimpleMarketClearing()
  {
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
    /* Todo accountingService is failing since last commit changes
     accountingService.activate(timeService.currentTime, 3)
     MarketPosition mpBuyer = MarketPosition.findByBrokerAndTimeslot(sampleBuyer, sampleTimeslot)
     MarketPosition mpSeller = MarketPosition.findByBrokerAndTimeslot(sampleSeller, sampleTimeslot)
     assertEquals(10.0, mpBuyer.overallBalance)
     assertEquals(-10.0, mpSeller.overallBalance)
     //assertEquals(-110.0, mpBuyer.broker.cash.overallBalance)
     //assertEquals(110.0, mpSeller.broker.cash.overallBalance)
     */
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

  void testClearingWithoutShouts() 
  {
    auctionService.clearMarket()

    Orderbook ob = Orderbook.findByTimeslot(sampleTimeslot)
    assertNotNull(ob)
    assertEquals(0, ob.asks.size())
    assertEquals(0, ob.bids.size())

    ClearedTrade ct = ClearedTrade.findByTimeslot(sampleTimeslot)
    assertNull(ct)

  }

  void testProcessShoutForDisabledTimeslot() 
  {
    Timeslot ts = Timeslot.findBySerialNumber(3)
    buyShout.timeslot = ts
    buyShout.limitPrice = 1.0
    buyShout.quantity = 2.0

    auctionService.processShout(buyShout)

    /** assert that invalid shout was not persisted  */
    assertEquals(0, Shout.list().size())
  }

  void testMultipleClearingRoundsAndBiddingJustForSingleRound() 
  {
    /** Round 1 */
    Shout ss1 = new Shout(sellShout.properties)
    ss1.limitPrice = 11.0
    ss1.quantity = 20.0
    auctionService.processShout(ss1)

    Shout bs1 = new Shout(buyShout.properties)
    bs1.limitPrice = 11.0
    bs1.quantity = 10.0
    auctionService.processShout(bs1)

    Shout bs2 = new Shout(buyShout2.properties)
    bs2.limitPrice = 2.0
    bs2.quantity = 10.0
    auctionService.processShout(bs2)

    auctionService.activate(timeService.currentTime, 3)

    // Validate persisted obejcts
    assertEquals(3, Shout.list().size())

    Shout s3 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('timeslot', sampleTimeslot)
      eq('limitPrice', 11.0)
      eq('quantity', 10.0)
      eq('modReasonCode', ModReasonCode.PARTIALEXECUTION)
    }
    assertNotNull(s3)
    assertEquals(BuySellIndicator.SELL, s3.buySellIndicator)
    assertEquals(10.0, s3.executionQuantity)
    assertEquals(11.0, s3.executionPrice)

    Shout s4 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('timeslot', sampleTimeslot)
      eq('limitPrice', 11.0)
      eq('quantity', 0.0)
      eq('modReasonCode', ModReasonCode.EXECUTION)
    }
    assertNotNull(s4)
    assertEquals(BuySellIndicator.BUY, s4.buySellIndicator)
    assertEquals(10.0, s4.executionQuantity)
    assertEquals(11.0, s4.executionPrice)

    Shout s5 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('timeslot', sampleTimeslot)
      eq('limitPrice', 2.0)
      eq('quantity', 10.0)
      eq('modReasonCode', ModReasonCode.DELETIONBYSYSTEM)
    }
    assertNotNull(s5)
    assertEquals(BuySellIndicator.BUY, s5.buySellIndicator)

    assertEquals(1, ClearedTrade.list().size())
    ClearedTrade persistedCt = ClearedTrade.findByTimeslot(sampleTimeslot)
    assertNotNull(persistedCt)
    assertEquals(11.0, persistedCt.executionPrice)
    assertEquals(10.0, persistedCt.executionQuantity)

    simulateTransition()

    /** Round 2  */
    Timeslot secondTs = Timeslot.findBySerialNumber(6)

    ss1 = new Shout(sellShout.properties)
    ss1.limitPrice = 11.0
    ss1.quantity = 20.0
    ss1.timeslot = secondTs
    auctionService.processShout(ss1)

    bs1 = new Shout(buyShout.properties)
    bs1.limitPrice = 11.0
    bs1.quantity = 10.0
    bs1.timeslot = secondTs
    auctionService.processShout(bs1)

    bs2 = new Shout(buyShout.properties)
    bs2.limitPrice = 2.0
    bs2.quantity = 10.0
    bs2.timeslot = secondTs
    auctionService.processShout(bs2)

    auctionService.activate(timeService.currentTime, 3)

    Shout s7 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('timeslot', secondTs)
      eq('limitPrice', 11.0)
      eq('quantity', 10.0)
      eq('modReasonCode', ModReasonCode.PARTIALEXECUTION)
    }
    assertNotNull(s7)
    assertEquals(BuySellIndicator.SELL, s7.buySellIndicator)
    assertEquals(10.0, s7.executionQuantity)
    assertEquals(11.0, s7.executionPrice)

    Shout s8 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('timeslot', secondTs)
      eq('limitPrice', 11.0)
      eq('quantity', 0.0)
      eq('modReasonCode', ModReasonCode.EXECUTION)
    }
    assertNotNull(s8)
    assertEquals(BuySellIndicator.BUY, s8.buySellIndicator)
    assertEquals(10.0, s8.executionQuantity)
    assertEquals(11.0, s8.executionPrice)

    Shout s9 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('timeslot', secondTs)
      eq('limitPrice', 2.0)
      eq('quantity', 10.0)
      eq('modReasonCode', ModReasonCode.DELETIONBYSYSTEM)
    }
    assertNotNull(s9)
    assertEquals(BuySellIndicator.BUY, s9.buySellIndicator)

    assertEquals(2, ClearedTrade.list().size())
    ClearedTrade secondCt = ClearedTrade.findByTimeslot(secondTs)
    assertNotNull(secondCt)
    assertEquals(11.0, secondCt.executionPrice)
    assertEquals(10.0, secondCt.executionQuantity)
  }


  void testMultipleClearingRoundsAndBiddingForMultipleRounds() 
  {
    /** Round 1 */

    /** bids for next enabled timeslot (serialnumber 5)*/
    sellShout.limitPrice = 11.0
    sellShout.quantity = 20.0
    auctionService.processShout(new Shout(sellShout.properties))

    buyShout.limitPrice = 11.0
    buyShout.quantity = 10.0
    auctionService.processShout(new Shout(buyShout.properties))

    buyShout2.limitPrice = 10.0
    buyShout2.quantity = 10.0
    auctionService.processShout(new Shout(buyShout2.properties))

    /** bids for future enabled timeslot (serialnumber 6)*/
    Timeslot ts = Timeslot.findBySerialNumber(6)

    sellShout.timeslot = ts
    auctionService.processShout(sellShout)
    buyShout.timeslot = ts
    auctionService.processShout(buyShout)
    buyShout2.timeslot = ts
    auctionService.processShout(buyShout2)

    auctionService.activate(timeService.currentTime, 3)

    // Validate persisted obejcts
    assertEquals(6, Shout.list().size())

    Shout s3 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('timeslot', sampleTimeslot)
      eq('limitPrice', 11.0)
      eq('quantity', 10.0)
      eq('modReasonCode', ModReasonCode.PARTIALEXECUTION)
    }
    assertNotNull(s3)
    assertEquals(BuySellIndicator.SELL, s3.buySellIndicator)
    assertEquals(10.0, s3.executionQuantity)
    assertEquals(11.0, s3.executionPrice)

    Shout s4 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('timeslot', sampleTimeslot)
      eq('limitPrice', 11.0)
      eq('quantity', 0.0)
      eq('modReasonCode', ModReasonCode.EXECUTION)
    }
    assertNotNull(s4)
    assertEquals(BuySellIndicator.BUY, s4.buySellIndicator)
    assertEquals(10.0, s4.executionQuantity)
    assertEquals(11.0, s4.executionPrice)

    Shout s5 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('timeslot', sampleTimeslot)
      eq('limitPrice', 10.0)
      eq('quantity', 10.0)
      eq('modReasonCode', ModReasonCode.DELETIONBYSYSTEM)
    }
    assertNotNull(s5)
    assertEquals(BuySellIndicator.BUY, s5.buySellIndicator)

    Shout s7 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('timeslot', ts)
      eq('limitPrice', 11.0)
      eq('quantity', 10.0)
      eq('modReasonCode', ModReasonCode.PARTIALEXECUTION)
    }
    assertNotNull(s7)
    assertEquals(BuySellIndicator.SELL, s7.buySellIndicator)
    assertEquals(10.0, s7.executionQuantity)
    assertEquals(11.0, s7.executionPrice)

    Shout s8 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('timeslot', ts)
      eq('limitPrice', 11.0)
      eq('quantity', 0.0)
      eq('modReasonCode', ModReasonCode.EXECUTION)
    }
    assertNotNull(s8)
    assertEquals(BuySellIndicator.BUY, s8.buySellIndicator)
    assertEquals(10.0, s8.executionQuantity)
    assertEquals(11.0, s8.executionPrice)

    Shout s9 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('timeslot', ts)
      eq('limitPrice', 10.0)
      eq('quantity', 10.0)
      eq('modReasonCode', ModReasonCode.DELETIONBYSYSTEM)
    }
    assertNotNull(s9)
    assertEquals(BuySellIndicator.BUY, s9.buySellIndicator)

    assertEquals(2, ClearedTrade.list().size())

    ClearedTrade persistedCt = ClearedTrade.findByTimeslot(sampleTimeslot)
    assertNotNull(persistedCt)
    assertEquals(11.0, persistedCt.executionPrice)
    assertEquals(10.0, persistedCt.executionQuantity)

    ClearedTrade secondCt = ClearedTrade.findByTimeslot(ts)
    assertNotNull(secondCt)
    assertEquals(11.0, secondCt.executionPrice)
    assertEquals(10.0, secondCt.executionQuantity)
  }

  void testShoutDeletionBetweenRoundsForFutureBuyShout() 
  {
    sellShout.limitPrice = 11.0
    sellShout.quantity = 20.0
    sellShout.timeslot = futureTimeslot
    auctionService.processShout(sellShout)

    auctionService.activate(timeService.currentTime, 3)
    simulateTransition()

    buyShout.limitPrice = 11.0
    buyShout.quantity = 10.0
    buyShout.timeslot = futureTimeslot
    auctionService.processShout(buyShout)

    auctionService.activate(timeService.currentTime, 3)

    assertEquals(2, Shout.list().size())
    Shout.list().collect {it -> assertEquals(ModReasonCode.DELETIONBYSYSTEM, it.modReasonCode)}

    assertEquals(0, ClearedTrade.list().size())
  }

  void testShoutDeletionBetweenRoundsForFutureSellShout() 
  {
    buyShout.limitPrice = 11.0
    buyShout.quantity = 10.0
    buyShout.timeslot = futureTimeslot
    auctionService.processShout(buyShout)

    auctionService.activate(timeService.currentTime, 3)
    simulateTransition()

    sellShout.limitPrice = 11.0
    sellShout.quantity = 20.0
    sellShout.timeslot = futureTimeslot
    auctionService.processShout(sellShout)

    auctionService.activate(timeService.currentTime, 3)

    assertEquals(2, Shout.list().size())
    Shout.list().collect {it -> assertEquals(ModReasonCode.DELETIONBYSYSTEM, it.modReasonCode)}

    assertEquals(0, ClearedTrade.list().size())
  }


  void testOrderbookInMarketClearinWithTwoBids() {
    buyShout.limitPrice = 13.0
    buyShout.quantity = 100.0
    buyShout.timeslot = sampleTimeslot
    auctionService.processShout(buyShout)

    buyShout.limitPrice = 11.0
    buyShout.quantity = 10.0
    buyShout.timeslot = sampleTimeslot
    auctionService.processShout(buyShout)

    auctionService.activate(timeService.currentTime, 3)

    Orderbook ob = Orderbook.findByProductAndTimeslot(sampleProduct, sampleTimeslot)

    assertEquals(2, ob.bids.size())
    assertEquals(0, ob.asks.size())

    assertEquals(13, ob.bids.first().limitPrice)
    assertEquals(100, ob.bids.first().quantity)
  }

  void testOrderbookInMarketClearinWithTwoAsks() {
    sellShout.limitPrice = 15.0
    sellShout.quantity = 10.0
    sellShout.timeslot = sampleTimeslot
    auctionService.processShout(sellShout)

    sellShout.limitPrice = 13.0
    sellShout.quantity = 50.0
    sellShout.timeslot = sampleTimeslot
    auctionService.processShout(sellShout)

    auctionService.activate(timeService.currentTime, 3)

    Orderbook ob = Orderbook.findByProductAndTimeslot(sampleProduct, sampleTimeslot)

    assertEquals(0, ob.bids.size())
    assertEquals(2, ob.asks.size())

    assertEquals(13, ob.asks.first().limitPrice)
    assertEquals(50, ob.asks.first().quantity)
  }



}
