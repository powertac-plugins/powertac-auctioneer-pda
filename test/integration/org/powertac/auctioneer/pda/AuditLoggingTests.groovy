package org.powertac.auctioneer.pda

import grails.test.GrailsUnitTestCase
import org.powertac.common.Shout
import org.joda.time.Instant
import org.powertac.common.Broker
import org.powertac.common.Product
import org.powertac.common.enumerations.ProductType
import org.powertac.common.Timeslot
import org.powertac.common.enumerations.BuySellIndicator
import org.powertac.common.enumerations.OrderType
import org.powertac.common.enumerations.ModReasonCode
import org.codehaus.groovy.grails.plugins.orm.auditable.AuditLogEvent

class AuditLoggingTests extends GrailsUnitTestCase {

  def sampleTimeslot
  def sampleProduct
  def sampleSeller
  def sampleBuyer
  Shout buyShout

  def timeService

  protected void setUp() {
    super.setUp()
    AuditLogEvent.list()*.delete()

    timeService.currentTime = new Instant()
    sampleSeller = new Broker(username: "SampleSeller")
    assert (sampleSeller.save())
    sampleBuyer = new Broker(username: "SampleBuyer")
    assert (sampleBuyer.save())
    sampleProduct = new Product(productType: ProductType.Future)
    assert (sampleProduct.save())
    sampleTimeslot = new Timeslot(serialNumber: 1, enabled: true, startInstant: new Instant(), endInstant: new Instant())
    assert (sampleTimeslot.validate())
    assert (sampleTimeslot.save())
    buyShout = new Shout(broker: sampleBuyer, product: sampleProduct, timeslot: sampleTimeslot, buySellIndicator: BuySellIndicator.BUY, orderType: OrderType.LIMIT, modReasonCode: ModReasonCode.INSERT)

  }

  protected void tearDown() {
    super.tearDown()
  }

  void testUpdateOfModReasonCode() {

    buyShout.limitPrice = 10.0
    buyShout.quantity = 20.0
    buyShout.transactionId = 1

    assert (buyShout.save(flush: true))


    buyShout.modReasonCode = ModReasonCode.EXECUTION
    assert (buyShout.save())

    println "Audit record count: ${AuditLogEvent.count()}"
    //AuditLogEvent.list().each { println it.toString() }
    def trace = AuditLogEvent.findAllByClassNameAndPersistedObjectId(buyShout.getClass().getName(), buyShout.id)
    trace.each { println "Log ${it.className} ${it.persistedObjectId}, prop:${it.propertyName} was ${it.oldValue}, now ${it.newValue}" }


  }

}
