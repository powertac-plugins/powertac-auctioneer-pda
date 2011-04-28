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

import grails.test.GrailsUnitTestCase
import org.powertac.common.Shout
import org.joda.time.Instant
import org.powertac.common.Broker
//import org.powertac.common.Product
import org.powertac.common.enumerations.ProductType
import org.powertac.common.Timeslot
import org.powertac.common.enumerations.BuySellIndicator
import org.powertac.common.enumerations.OrderType
import org.powertac.common.enumerations.ModReasonCode
import org.codehaus.groovy.grails.plugins.orm.auditable.AuditLogEvent

/**
 * Testing the AuditLogging Plugin within the auctioneer environment.
 *
 * @author Daniel Schnurr
 */

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
    sampleProduct = ProductType.Future
    assert (sampleProduct.save())
    sampleTimeslot = new Timeslot(serialNumber: 1, enabled: true, startInstant: new Instant(), endInstant: new Instant())
    assert (sampleTimeslot.validate())
    assert (sampleTimeslot.save())
    buyShout = new Shout(broker: sampleBuyer, product: sampleProduct, timeslot: sampleTimeslot, buySellIndicator: BuySellIndicator.BUY, modReasonCode: ModReasonCode.INSERT)

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
    assert (buyShout.save(flush:true))

    buyShout.limitPrice = 15.0
    assert (buyShout.save(flush:true))

    buyShout.quantity = 33.0
    assert (buyShout.save(flush:true))

    println "Audit record count: ${AuditLogEvent.count()}"
    //AuditLogEvent.list().each { println it.toString() }
    def trace = AuditLogEvent.findAllByClassNameAndPersistedObjectId(buyShout.getClass().getName(), buyShout.id)
    assertEquals(4, trace.size())
    trace.each { println "Log ${it.className} ${it.persistedObjectId}, prop:${it.propertyName} was ${it.oldValue}, now ${it.newValue}" }


  }

}
