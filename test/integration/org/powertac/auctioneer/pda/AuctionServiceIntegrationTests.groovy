package org.powertac.auctioneer.pda

import grails.test.GrailsUnitTestCase
import org.powertac.common.command.ShoutDoCreateCmd
import org.powertac.common.Competition
import org.powertac.common.Broker
import org.powertac.common.Product
import org.powertac.common.enumerations.ProductType
import org.powertac.common.enumerations.BuySellIndicator
import org.powertac.common.enumerations.OrderType
import org.powertac.common.Shout
import org.powertac.common.enumerations.ModReasonCode
import org.powertac.common.Timeslot
import org.powertac.common.command.ShoutDoDeleteCmd
import org.powertac.common.command.ShoutDoUpdateCmd
import org.powertac.common.command.CashDoUpdateCmd
import org.powertac.common.command.PositionDoUpdateCmd
import org.powertac.common.TransactionLog
import org.powertac.common.enumerations.TransactionType
import javax.transaction.Transaction
import org.powertac.common.Orderbook

class AuctionServiceIntegrationTests extends GrailsUnitTestCase {

  def competition
  def sampleTimeslot
  def sampleProduct
  def sampleSeller
  def sampleBuyer
  ShoutDoCreateCmd sell1
  ShoutDoCreateCmd buy1
  Shout buyShout
  Shout buyShout2
  Shout sellShout

  def auctionService

  protected void setUp() {
    super.setUp()

    competition = new Competition(name: "sampleCompetition", enabled: true, current: true)
    assert (competition.save())
    sampleSeller = new Broker(userName: "SampleSeller", competition: competition)
    assert (sampleSeller.save())
    sampleBuyer = new Broker(userName: "SampleBuyer", competition: competition)
    assert (sampleBuyer.save())
    sampleProduct = new Product(competition: competition, productType: ProductType.Future)
    assert (sampleProduct.save())
    sampleTimeslot = new Timeslot(serialNumber: 1, competition: competition, enabled: true)
    assert (sampleTimeslot.save())
    sell1 = new ShoutDoCreateCmd(competition: competition, broker: sampleSeller, product: sampleProduct, timeslot: sampleTimeslot, buySellIndicator: BuySellIndicator.SELL, orderType: OrderType.LIMIT)
    buy1 = new ShoutDoCreateCmd(competition: competition, broker: sampleBuyer, product: sampleProduct, timeslot: sampleTimeslot, buySellIndicator: BuySellIndicator.BUY, orderType: OrderType.LIMIT)

    buyShout = new Shout(competition: competition, broker: sampleBuyer, product: sampleProduct, timeslot: sampleTimeslot, buySellIndicator: BuySellIndicator.BUY, orderType: OrderType.LIMIT, modReasonCode: ModReasonCode.INSERT, latest: true)
    buyShout2 = new Shout(competition: competition, broker: sampleBuyer, product: sampleProduct, timeslot: sampleTimeslot, buySellIndicator: BuySellIndicator.BUY, orderType: OrderType.LIMIT, modReasonCode: ModReasonCode.INSERT, latest: true)
    sellShout = new Shout(competition: competition, broker: sampleSeller, product: sampleProduct, timeslot: sampleTimeslot, buySellIndicator: BuySellIndicator.SELL, orderType: OrderType.LIMIT, modReasonCode: ModReasonCode.INSERT, latest: true)
  }

  protected void tearDown() {
    super.tearDown()
  }

  void testIncomingSellShoutDoCreateCmd() {
    sell1.limitPrice = 10.0
    sell1.quantity = 10.0
    auctionService.processShoutCreate(sell1)

    assertEquals(1, Shout.list().size())
    Shout s1 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 10.0)
      eq('quantity', 10.0)
    }
    assertNotNull s1
    assertEquals(ModReasonCode.INSERT, s1.modReasonCode)
    assertNotNull s1.shoutId
    assertNotNull s1.transactionId

  }

  void testIncomingBuyShoutDoCreateCmd() {
    buy1.limitPrice = 12.0
    buy1.quantity = 10.0
    auctionService.processShoutCreate(buy1)

    assertEquals(1, Shout.list().size())
    Shout b1 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 12.0)
      eq('quantity', 10.0)
    }
    assertNotNull b1
    assertEquals(ModReasonCode.INSERT, b1.modReasonCode)
    assertNotNull b1.shoutId
    assertNotNull b1.transactionId

  }

  void testIncomingBuyAndSellShoutDoCreateCmd() {
    sell1.limitPrice = 10.0
    sell1.quantity = 10.0
    auctionService.processShoutCreate(sell1)

    buy1.limitPrice = 12.0
    buy1.quantity = 10.0
    auctionService.processShoutCreate(buy1)

    assertEquals(2, Shout.list().size())

    Shout s1 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 10.0)
      eq('quantity', 10.0)
    }
    assertNotNull s1
    assertEquals(ModReasonCode.INSERT, s1.modReasonCode)
    assertNotNull s1.shoutId
    assertNotNull s1.transactionId

    Shout b1 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 12.0)
      eq('quantity', 10.0)
    }
    assertNotNull b1
    assertEquals(ModReasonCode.INSERT, b1.modReasonCode)
    assertNotNull b1.shoutId
    assertNotNull b1.transactionId
  }


  void testDeletionOfShout() {
    //init
    sell1.limitPrice = 10.0
    sell1.quantity = 10.0
    auctionService.processShoutCreate(sell1)

    assertEquals(1, Shout.list().size())
    Shout s1 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 10.0)
      eq('quantity', 10.0)
    }
    assertNotNull s1

    //action
    def delSell1 = new ShoutDoDeleteCmd(competition: competition, broker: sampleSeller, shoutId: s1.shoutId)
    auctionService.processShoutDelete(delSell1)

    //validate
    assertEquals(2, Shout.list().size())

    Shout oldS1 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 10.0)
      eq('quantity', 10.0)
      eq('modReasonCode', ModReasonCode.INSERT)
      eq('latest', false)
    }
    assertNotNull oldS1
    assertEquals(s1.shoutId, oldS1.shoutId)

    Shout delS1 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 10.0)
      eq('quantity', 10.0)
      eq('modReasonCode', ModReasonCode.DELETIONBYUSER)
      eq('latest', false)
    }
    assertNotNull delS1
    assertEquals(s1.shoutId, delS1.shoutId)
    assertNotNull delS1.transactionId
    assertNotSame(oldS1.transactionId, delS1.transactionId)
  }

  void testUpdateOfShout() {
    //init
    sell1.limitPrice = 10.0
    sell1.quantity = 10.0
    auctionService.processShoutCreate(sell1)

    assertEquals(1, Shout.list().size())
    Shout s1 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 10.0)
      eq('quantity', 10.0)
    }
    assertNotNull s1

    //action
    def updateSell1 = new ShoutDoUpdateCmd(competition: competition, broker: sampleSeller, shoutId: s1.shoutId, quantity: 20.0, limitPrice: 9.0)
    auctionService.processShoutUpdate(updateSell1)

    //validate
    assertEquals(3, Shout.list().size())

    Shout oldInsertedS1 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 10.0)
      eq('quantity', 10.0)
      eq('modReasonCode', ModReasonCode.INSERT)
      eq('latest', false)
    }
    assertNotNull oldInsertedS1
    assertEquals(s1.shoutId, oldInsertedS1.shoutId)

    Shout oldDeletedS1 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 10.0)
      eq('quantity', 10.0)
      eq('modReasonCode', ModReasonCode.DELETIONBYUSER)
      eq('latest', false)
    }
    assertNotNull oldDeletedS1
    assertEquals(s1.shoutId, oldDeletedS1.shoutId)
    assertNotSame(oldInsertedS1.transactionId, oldDeletedS1.transactionId)

    Shout updatedS1 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 9.0)
      eq('quantity', 20.0)
      eq('latest', true)
    }
    assertNotNull updatedS1
    assertEquals(ModReasonCode.MODIFICATION, updatedS1.modReasonCode)
    assertEquals(s1.shoutId, updatedS1.shoutId)
    assertNotNull updatedS1.transactionId
    assertNotSame(oldInsertedS1.transactionId, updatedS1.transactionId)
    assertNotSame(oldDeletedS1.transactionId, updatedS1.transactionId)

  }

  void testSettlementForBuyShout() {
    buyShout.executionQuantity = 100
    buyShout.executionPrice = 10

    def cashUpdate = auctionService.settleCashUpdate(buyShout)

    assert (cashUpdate instanceof CashDoUpdateCmd)
    assertEquals(sampleBuyer, cashUpdate.broker)
    assertEquals(competition, cashUpdate.competition)
    assertEquals(-buyShout.executionQuantity * buyShout.executionPrice, cashUpdate.relativeChange)
    assertEquals("org.powertac.auctioneer.pda", cashUpdate.origin)

    def posUpdate = auctionService.settlePositionUpdate(buyShout)
    assert (posUpdate instanceof PositionDoUpdateCmd)
    assertEquals(sampleBuyer, posUpdate.broker)
    assertEquals(competition, posUpdate.competition)
    assertEquals(buyShout.executionQuantity, posUpdate.relativeChange)
    assertEquals("org.powertac.auctioneer.pda", posUpdate.origin)
  }

  void testSettlementForSellShout() {
    sellShout.executionQuantity = 100
    sellShout.executionPrice = 10

    def cashUpdate = auctionService.settleCashUpdate(sellShout)

    assert (cashUpdate instanceof CashDoUpdateCmd)
    assertEquals(sampleSeller, cashUpdate.broker)
    assertEquals(competition, cashUpdate.competition)
    assertEquals(sellShout.executionQuantity * sellShout.executionPrice, cashUpdate.relativeChange)
    assertEquals("org.powertac.auctioneer.pda", cashUpdate.origin)

    def posUpdate = auctionService.settlePositionUpdate(sellShout)
    assert (posUpdate instanceof PositionDoUpdateCmd)
    assertEquals(sampleSeller, posUpdate.broker)
    assertEquals(competition, posUpdate.competition)
    assertEquals(-sellShout.executionQuantity, posUpdate.relativeChange)
    assertEquals("org.powertac.auctioneer.pda", posUpdate.origin)
  }

  void testTradeLog() {
    //init
    Map stat = ['executableVolume': 100.0, 'price': 15.0, 'product': sampleProduct, 'competition': competition, 'timeslot': sampleTimeslot, 'transactionId': "123abc"]
    TransactionLog returnedTl = auctionService.writeTradeLog(stat)

    //validate
    assertEquals(1, TransactionLog.list().size())
    TransactionLog persistedTl = TransactionLog.list().first()
    assert persistedTl.latest
    assertEquals(TransactionType.TRADE, persistedTl.transactionType)
    assertEquals(stat.price, persistedTl.price)
    assertEquals(stat.executableVolume, persistedTl.quantity)
    assertEquals(sampleProduct, persistedTl.product)
    assertEquals(competition, persistedTl.competition)
    assertEquals(stat.transactionId, persistedTl.transactionId)

    assert returnedTl.latest
    assertEquals(TransactionType.TRADE, returnedTl.transactionType)
    assertEquals(stat.price, returnedTl.price)
    assertEquals(stat.executableVolume, returnedTl.quantity)
    assertEquals(sampleProduct, returnedTl.product)
    assertEquals(competition, returnedTl.competition)
    assertEquals(stat.transactionId, returnedTl.transactionId)
  }

  void testSimpleUpdateOfEmptyOrderbook() {
    //init
    buyShout.quantity = 50
    buyShout.limitPrice = 11
    buyShout.shoutId = "111"
    buyShout.transactionId = "tbd"
    buyShout.save()
    auctionService.updateOrderbook(buyShout)

    sellShout.quantity = 20
    sellShout.limitPrice = 13
    sellShout.shoutId = "112"
    sellShout.transactionId = "tbd2"
    sellShout.save()

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
  }

  /*
   * Incoming shout (bid) adds quantity to existing price level
   */

  void testAggregationUpdateOfEmptyOrderbook() {
    //init
    buyShout.quantity = 50
    buyShout.limitPrice = 11
    buyShout.shoutId = "111"
    buyShout.transactionId = "tbd"
    buyShout.save()
    auctionService.updateOrderbook(buyShout)

    sellShout.quantity = 20
    sellShout.limitPrice = 13
    sellShout.shoutId = "112"
    sellShout.transactionId = "tbd2"
    sellShout.save()
    auctionService.updateOrderbook(sellShout)

    //action
    buyShout2.quantity = 15
    buyShout2.limitPrice = 11
    buyShout2.shoutId = "113"
    buyShout2.transactionId = "tbd3"
    buyShout2.save()
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

  void testSimpleUpdateOfFirstQuoteWithBidOnly() {
    //init
    Orderbook orderbook = new Orderbook(competition: competition, product: sampleProduct, timeslot: sampleTimeslot, transactionId: "123", latest: true, bid0: 10.0, bidSize0: 20)

    //action
    TransactionLog tl = auctionService.updateQuote(orderbook)

    //validate
    assertNotNull(tl)
    assertEquals(orderbook.bid0, tl.bid)
    assertEquals(orderbook.bidSize0, tl.bidSize)
    assertNull(tl.ask)
    assertNull(tl.askSize)
    assertEquals(orderbook.competition, tl.competition)
    assertEquals(orderbook.timeslot, tl.timeslot)
    assertEquals(orderbook.product, tl.product)
    assertEquals(orderbook.transactionId, tl.transactionId)
    assert (tl.latest)
  }

  void testSimpleUpdateOfFirstQuoteWithAskOnly() {
    //init
    Orderbook orderbook = new Orderbook(competition: competition, product: sampleProduct, timeslot: sampleTimeslot, transactionId: "123", latest: true, ask0: 10.0, askSize0: 20)

    //action
    TransactionLog tl = auctionService.updateQuote(orderbook)

    //validate
    assertNotNull(tl)
    assertEquals(orderbook.ask0, tl.ask)
    assertEquals(orderbook.askSize0, tl.askSize)
    assertNull(tl.bid)
    assertNull(tl.bidSize)
    assertEquals(orderbook.competition, tl.competition)
    assertEquals(orderbook.timeslot, tl.timeslot)
    assertEquals(orderbook.product, tl.product)
    assertEquals(orderbook.transactionId, tl.transactionId)
    assert (tl.latest)
  }

  void testSimpleUpdateOfFirstQuoteWithBidAndAsk() {
    //init
    Orderbook orderbook = new Orderbook(competition: competition, product: sampleProduct, timeslot: sampleTimeslot, transactionId: "123", latest: true, bid0: 10.0, bidSize0: 20, ask0: 13, askSize0: 10)

    //action
    TransactionLog tl = auctionService.updateQuote(orderbook)

    //validate
    assertNotNull(tl)
    assertEquals(orderbook.bid0, tl.bid)
    assertEquals(orderbook.bidSize0, tl.bidSize)
    assertEquals(orderbook.ask0, tl.ask)
    assertEquals(orderbook.askSize0, tl.askSize)
    assertEquals(orderbook.competition, tl.competition)
    assertEquals(orderbook.timeslot, tl.timeslot)
    assertEquals(orderbook.product, tl.product)
    assertEquals(orderbook.transactionId, tl.transactionId)
    assert (tl.latest)
  }

  void testUpdateQuoteAfterIncomingShoutDoCreateCommandSequence() {
    //init + action
    sell1.limitPrice = 13.0
    sell1.quantity = 10.0
    TransactionLog firstTl = auctionService.processShoutCreate(sell1).findAll {it instanceof TransactionLog}.first()

    buy1.limitPrice = 10.0
    buy1.quantity = 10.0
    TransactionLog secondTl = auctionService.processShoutCreate(buy1).findAll {it instanceof TransactionLog}.first()

    sell1.limitPrice = 12.0
    sell1.quantity = 20
    TransactionLog thirdTl = auctionService.processShoutCreate(sell1).findAll {it instanceof TransactionLog}.first()

    buy1.limitPrice = 10.0
    buy1.quantity = 30.0
    TransactionLog fourthTl = auctionService.processShoutCreate(buy1).findAll {it instanceof TransactionLog}.first()

    //validate
    assertNotNull(firstTl)
    assertEquals(13.0, firstTl.ask)
    assertEquals(10.0, firstTl.askSize)
    assertNull(firstTl.bid)
    assertNull(firstTl.bidSize)

    assertNotNull(secondTl)
    assertEquals(13.0, secondTl.ask)
    assertEquals(10.0, secondTl.askSize)
    assertEquals(10.0, secondTl.bid)
    assertEquals(10.0, secondTl.bidSize)

    assertNotNull(thirdTl)
    assertEquals(12.0, thirdTl.ask)
    assertEquals(20.0, thirdTl.askSize)
    assertEquals(10.0, thirdTl.bid)
    assertEquals(10.0, thirdTl.bidSize)

    assertNotNull(fourthTl)
    assertEquals(12.0, fourthTl.ask)
    assertEquals(20.0, fourthTl.askSize)
    assertEquals(10.0, fourthTl.bid)
    assertEquals(40.0, fourthTl.bidSize)

    TransactionLog firstPersistedTl = (TransactionLog) TransactionLog.withCriteria(uniqueResult: true) {
      eq('ask', 13.0)
      eq('askSize', 10.0)
      isNull('bid')
      isNull('bidSize')
    }

    assertNotNull(firstPersistedTl)
    assertFalse(firstPersistedTl.latest)

    TransactionLog secondPersistedTl = (TransactionLog) TransactionLog.withCriteria(uniqueResult: true) {
      eq('ask', 13.0)
      eq('askSize', 10.0)
      eq('bid', 10.0)
      eq('bidSize', 10.0)
    }

    assertNotNull(secondPersistedTl)
    assertFalse(secondPersistedTl.latest)

    TransactionLog thirdPersistedTl = (TransactionLog) TransactionLog.withCriteria(uniqueResult: true) {
      eq('ask', 12.0)
      eq('askSize', 20.0)
      eq('bid', 10.0)
      eq('bidSize', 10.0)
    }

    assertNotNull(thirdPersistedTl)
    assertFalse(thirdPersistedTl.latest)

    TransactionLog fourthPersistedTl = (TransactionLog) TransactionLog.withCriteria(uniqueResult: true) {
      eq('ask', 12.0)
      eq('askSize', 20.0)
      eq('bid', 10.0)
      eq('bidSize', 40.0)
    }

    assertNotNull(fourthPersistedTl)
    assert(fourthPersistedTl.latest)
  }

  void testQuoteUpdateAfterShoutDeletion() {
    //init
    sell1.limitPrice = 15.0
    sell1.quantity = 10.0
    auctionService.processShoutCreate(sell1)

    def shoutId = Shout.findByLatest(true).shoutId
    ShoutDoDeleteCmd myShoutDoDeleteCmd = new ShoutDoDeleteCmd(competition: competition, broker: sampleSeller, shoutId: shoutId)

    //action
    List output = auctionService.processShoutDelete(myShoutDoDeleteCmd)
    assertNotNull(output)
    TransactionLog tlAfterDelete = output.findAll {it instanceof TransactionLog}.first()

    //validate
    assertNotNull(tlAfterDelete)
    assertNull(tlAfterDelete.bid)
    assertNull(tlAfterDelete.bidSize)
    assertNull(tlAfterDelete.ask)
    assertNull(tlAfterDelete.askSize)
    assert(tlAfterDelete.latest)
  }


  void testCompleteAllocationOfSingleBuyShout() {
    //init
    buyShout.quantity = 50
    buyShout.limitPrice = 11
    buyShout.shoutId = "111"
    buyShout.transactionId = "tbd"
    buyShout.save()

    Map stat = [:]
    stat.price = 10
    stat.executableVolume = 200
    stat.transactionId = "123"
    BigDecimal aggregQuantityAllocated = 100

    //action
    List results = auctionService.allocateSingleShout(buyShout, aggregQuantityAllocated, stat)
    //validate
    assert (results[0] instanceof Shout)
    Shout returnedShout = (Shout) results[0]
    assertEquals(stat.price, returnedShout.executionPrice)
    assertEquals(buyShout.quantity, returnedShout.executionQuantity)
    assertEquals(ModReasonCode.EXECUTION, returnedShout.modReasonCode)
    assertEquals(stat.transactionId, returnedShout.transactionId)
    assertEquals("Matched by org.powertac.auctioneer.pda", returnedShout.comment)

    assert (results[1] instanceof BigDecimal)
    assertEquals(aggregQuantityAllocated + buyShout.quantity, results[1])

    assertEquals(2, Shout.list().size())

    Shout persistedShout = (Shout) Shout.findByLatestAndShoutId(true, buyShout.shoutId)
    assertEquals(stat.price, persistedShout.executionPrice)
    assertEquals(buyShout.quantity, persistedShout.executionQuantity)
    assertEquals(ModReasonCode.EXECUTION, persistedShout.modReasonCode)
    assertEquals(stat.transactionId, persistedShout.transactionId)
    assertEquals("Matched by org.powertac.auctioneer.pda", persistedShout.comment)
  }

  void testPartialAllocationOfSingleBuyShout() {
    //init
    buyShout.quantity = 100
    buyShout.limitPrice = 11
    buyShout.shoutId = "111"
    buyShout.transactionId = "tbd"
    buyShout.save()

    Map stat = [:]
    stat.price = 10
    stat.executableVolume = 100
    stat.transactionId = "123"
    BigDecimal aggregQuantityAllocated = 50

    //action
    List results = auctionService.allocateSingleShout(buyShout, aggregQuantityAllocated, stat)
    //validate
    assert (results[0] instanceof Shout)
    Shout returnedShout = (Shout) results[0]
    assertEquals(stat.price, returnedShout.executionPrice)
    assertEquals((stat.executableVolume - aggregQuantityAllocated), returnedShout.executionQuantity)
    assertEquals(buyShout.quantity - (stat.executableVolume - aggregQuantityAllocated), returnedShout.quantity)
    assertEquals(ModReasonCode.PARTIALEXECUTION, returnedShout.modReasonCode)
    assertEquals(stat.transactionId, returnedShout.transactionId)
    assertEquals("Matched by org.powertac.auctioneer.pda", returnedShout.comment)

    assert (results[1] instanceof BigDecimal)
    assertEquals(stat.executableVolume, results[1])

    assertEquals(2, Shout.list().size())

    Shout persistedShout = (Shout) Shout.findByLatestAndShoutId(true, buyShout.shoutId)
    assertEquals(stat.price, persistedShout.executionPrice)
    assertEquals((stat.executableVolume - aggregQuantityAllocated), persistedShout.executionQuantity)
    assertEquals(buyShout.quantity - (stat.executableVolume - aggregQuantityAllocated), persistedShout.quantity)
    assertEquals(ModReasonCode.PARTIALEXECUTION, persistedShout.modReasonCode)
    assertEquals(stat.transactionId, persistedShout.transactionId)
    assertEquals("Matched by org.powertac.auctioneer.pda", persistedShout.comment)
  }

  void testSimpleUniformPriceCalculation() {
    //init
    sell1.limitPrice = 11.0
    sell1.quantity = 20.0
    auctionService.processShoutCreate(sell1)

    buy1.limitPrice = 11.0
    buy1.quantity = 10.0
    auctionService.processShoutCreate(buy1)

    //action
    def shouts = Shout.list()
    Map stat = auctionService.calcUniformPrice(shouts)

    //validate
    assertEquals(11.0, stat.price)
    assertEquals(10.0, stat.executableVolume)
    assertEquals(20.0, stat.aggregatedQuantityAsk)
    assertEquals(10.0, stat.aggregatedQuantityBid)
  }

  void testUniformPriceCalculationWithHigherMinimumAskQuantityAtLowestPrice() {
    //init
    sell1.limitPrice = 11.0
    sell1.quantity = 20.0
    auctionService.processShoutCreate(sell1)

    sell1.limitPrice = 13.0
    sell1.quantity = 50.0
    auctionService.processShoutCreate(sell1)

    buy1.limitPrice = 13.0
    buy1.quantity = 10.0
    auctionService.processShoutCreate(buy1)

    buy1.limitPrice = 11.0
    buy1.quantity = 70.0
    auctionService.processShoutCreate(buy1)

    //action
    def shouts = Shout.list()
    Map stat = auctionService.calcUniformPrice(shouts)

    //validate
    assertEquals(11.0, stat.price)
    assertEquals(20.0, stat.executableVolume)
    assertEquals(20.0, stat.aggregatedQuantityAsk)
    assertEquals(80.0, stat.aggregatedQuantityBid)
  }

  void testUniformPriceCalculationWithHighestMinimumAskQuantityAndMinimalSurplus() {
    //init
    sell1.limitPrice = 11.0
    sell1.quantity = 20.0
    auctionService.processShoutCreate(sell1)

    sell1.limitPrice = 13.0
    sell1.quantity = 50.0
    auctionService.processShoutCreate(sell1)

    buy1.limitPrice = 13.0
    buy1.quantity = 10.0
    auctionService.processShoutCreate(buy1)

    buy1.limitPrice = 12.0
    buy1.quantity = 30.0
    auctionService.processShoutCreate(buy1)

    buy1.limitPrice = 11.0
    buy1.quantity = 40.0
    auctionService.processShoutCreate(buy1)

    //action
    def shouts = Shout.list()
    Map stat = auctionService.calcUniformPrice(shouts)

    //validate
    assertEquals(12.0, stat.price)
    assertEquals(20.0, stat.executableVolume)
    assertEquals(20.0, stat.aggregatedQuantityAsk)
    assertEquals(40.0, stat.aggregatedQuantityBid)
  }


  void testUniformPriceCalculationWithHighestMinimumBidQuantityAtMiddlePrice() {
    //init
    sell1.limitPrice = 11.0
    sell1.quantity = 20.0
    auctionService.processShoutCreate(sell1)

    sell1.limitPrice = 12.0
    sell1.quantity = 30.0
    auctionService.processShoutCreate(sell1)

    sell1.limitPrice = 13.0
    sell1.quantity = 20.0
    auctionService.processShoutCreate(sell1)

    buy1.limitPrice = 13.0
    buy1.quantity = 10.0
    auctionService.processShoutCreate(buy1)

    buy1.limitPrice = 12.0
    buy1.quantity = 30.0
    auctionService.processShoutCreate(buy1)

    buy1.limitPrice = 11.0
    buy1.quantity = 40.0
    auctionService.processShoutCreate(buy1)

    //action
    def shouts = Shout.list()
    Map stat = auctionService.calcUniformPrice(shouts)

    //validate
    assertEquals(12.0, stat.price)
    assertEquals(40.0, stat.executableVolume)
    assertEquals(50.0, stat.aggregatedQuantityAsk)
    assertEquals(40.0, stat.aggregatedQuantityBid)
  }


  void testSimpleMarketClearing() {
    //init
    sell1.limitPrice = 11.0
    sell1.quantity = 20.0
    auctionService.processShoutCreate(sell1)

    buy1.limitPrice = 11.0
    buy1.quantity = 10.0
    auctionService.processShoutCreate(buy1)

    assertEquals(2, Shout.list().size())
    //action
    List results = auctionService.clearMarket()

    // Validate persisted obejcts
    assertEquals(4, Shout.list().size())

    Shout s1 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 11.0)
      eq('quantity', 20.0)
      eq('modReasonCode', ModReasonCode.INSERT)
    }
    assertNotNull(s1)
    assertFalse(s1.latest)
    assertEquals(BuySellIndicator.SELL, s1.buySellIndicator)

    Shout s2 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 11.0)
      eq('quantity', 10.0)
      eq('modReasonCode', ModReasonCode.INSERT)
    }
    assertNotNull(s2)
    assertFalse(s2.latest)
    assertEquals(BuySellIndicator.BUY, s2.buySellIndicator)

    Shout s3 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 11.0)
      eq('quantity', 10.0)
      eq('modReasonCode', ModReasonCode.PARTIALEXECUTION)
    }
    assertNotNull(s3)
    assert (s3.latest)
    assertEquals(BuySellIndicator.SELL, s3.buySellIndicator)
    assertEquals(10.0, s3.executionQuantity)
    assertEquals(11.0, s3.executionPrice)

    Shout s4 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 11.0)
      eq('quantity', 0.0)
      eq('modReasonCode', ModReasonCode.EXECUTION)
    }
    assertNotNull(s4)
    assert (s4.latest)
    assertEquals(BuySellIndicator.BUY, s4.buySellIndicator)
    assertEquals(10.0, s4.executionQuantity)
    assertEquals(11.0, s4.executionPrice)

    // Validate returned list

    CashDoUpdateCmd cashUpdateBuyer = results.findAll {it instanceof CashDoUpdateCmd && it.relativeChange < 0}.first()
    assertEquals(sampleBuyer, cashUpdateBuyer.broker)
    assertEquals(competition, cashUpdateBuyer.competition)
    assertEquals(-110, cashUpdateBuyer.relativeChange)
    assertEquals("org.powertac.auctioneer.pda", cashUpdateBuyer.origin)

    PositionDoUpdateCmd posUpdateBuyer = results.findAll {it instanceof PositionDoUpdateCmd && it.relativeChange > 0}.first()
    assert (posUpdateBuyer instanceof PositionDoUpdateCmd)
    assertEquals(sampleBuyer, posUpdateBuyer.broker)
    assertEquals(competition, posUpdateBuyer.competition)
    assertEquals(10, posUpdateBuyer.relativeChange)
    assertEquals("org.powertac.auctioneer.pda", posUpdateBuyer.origin)

    CashDoUpdateCmd cashUpdateSeller = results.findAll {it instanceof CashDoUpdateCmd && it.relativeChange > 0}.first()
    assertEquals(sampleSeller, cashUpdateSeller.broker)
    assertEquals(competition, cashUpdateSeller.competition)
    assertEquals(+110, cashUpdateSeller.relativeChange)
    assertEquals("org.powertac.auctioneer.pda", cashUpdateSeller.origin)

    PositionDoUpdateCmd posUpdateSeller = results.findAll {it instanceof PositionDoUpdateCmd && it.relativeChange < 0}.first()
    assertEquals(sampleSeller, posUpdateSeller.broker)
    assertEquals(competition, posUpdateSeller.competition)
    assertEquals(-10, posUpdateSeller.relativeChange)
    assertEquals("org.powertac.auctioneer.pda", posUpdateSeller.origin)

    assertEquals(2, results.findAll {it instanceof Shout}.size())

    Shout updatedSell = results.findAll {it instanceof Shout && it.buySellIndicator == BuySellIndicator.SELL}.first()
    assertEquals(11.0, updatedSell.limitPrice)
    assertEquals(10.0, updatedSell.quantity)
    assertEquals(11.0, updatedSell.executionPrice)
    assertEquals(10.0, updatedSell.executionQuantity)
    assertEquals(ModReasonCode.PARTIALEXECUTION, updatedSell.modReasonCode)
    assertEquals(sampleSeller, updatedSell.broker)

    Shout updatedBuy = results.findAll {it instanceof Shout && it.buySellIndicator == BuySellIndicator.BUY}.first()
    assertEquals(11.0, updatedBuy.limitPrice)
    assertEquals(0.0, updatedBuy.quantity)
    assertEquals(11.0, updatedBuy.executionPrice)
    assertEquals(10.0, updatedBuy.executionQuantity)
    assertEquals(ModReasonCode.EXECUTION, updatedBuy.modReasonCode)
    assertEquals(sampleBuyer, updatedBuy.broker)

    TransactionLog tradeLog = results.findAll {it instanceof TransactionLog && it.transactionType == TransactionType.TRADE}.first()
    assertEquals(11.0, tradeLog.price)
    assertEquals(10.0, tradeLog.quantity)
    assertEquals(sampleProduct, tradeLog.product)
    assertEquals(competition, tradeLog.competition)
  }


}
