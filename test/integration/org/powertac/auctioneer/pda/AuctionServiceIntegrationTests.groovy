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

class AuctionServiceIntegrationTests extends GrailsUnitTestCase {

  def competition
  def sampleTimeslot
  def sampleProduct
  def sampleSeller
  def sampleBuyer
  ShoutDoCreateCmd sell1
  ShoutDoCreateCmd buy1
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
      eq('latest', false)
    }
    assertNotNull oldS1
    assertEquals(s1.shoutId, oldS1.shoutId)

    Shout delS1 = (Shout) Shout.withCriteria(uniqueResult: true) {
      eq('limitPrice', 10.0)
      eq('quantity', 10.0)
      eq('latest', true)
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

}
