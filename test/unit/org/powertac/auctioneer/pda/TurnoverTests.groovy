package org.powertac.auctioneer.pda

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

  void testGetSurplus() {
    Turnover t1 = new Turnover(price: 1.0, aggregatedQuantityAsk: 20.0, aggregatedQuantityBid: 25.0)
    assertEquals(5.0, t1.getSurplus())
    t1.aggregatedQuantityBid = 15.0
    assertEquals(5.0, t1.getSurplus())
    t1.aggregatedQuantityBid = 0.0
    assertEquals(20.0, t1.getSurplus())
    t1.aggregatedQuantityBid = -50.0
    assertEquals(20, t1.getSurplus())
  }

  void testSorting() {
    Turnover t1 = new Turnover(price: 1.0, aggregatedQuantityAsk: 20.0, aggregatedQuantityBid: 25.0)
    Turnover t2 = new Turnover(price: 1.0, aggregatedQuantityAsk: 20.0, aggregatedQuantityBid: 25.0)

    assertEquals(0, t1.compareTo(t2))
    t2.price = 1.5d
    assertEquals(0, t1.compareTo(t2))

    t1.aggregatedQuantityAsk = 25
    assertEquals(-1, t1.compareTo(t2))
    assertEquals(1, t2.compareTo(t1))
    def list = [t1, t2].sort()
    assertEquals(t1, list[0])

    t2.aggregatedQuantityAsk = 25
    assertEquals(0, t1.compareTo(t2))

    t2.aggregatedQuantityAsk = 26
    assertEquals(-1, t1.compareTo(t2))
    list.sort()
    assertEquals(t1, list[0])

    t2.aggregatedQuantityBid = 26
    list.sort()
    assertEquals(t2, list[0])

  }
}
