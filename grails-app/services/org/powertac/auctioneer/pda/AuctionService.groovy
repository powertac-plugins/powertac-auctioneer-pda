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

import org.powertac.common.interfaces.Auctioneer
import org.powertac.common.Shout

import org.powertac.common.enumerations.ModReasonCode
import org.powertac.common.IdGenerator

import org.powertac.common.enumerations.ProductType
import org.powertac.common.enumerations.BuySellIndicator

import org.powertac.common.Timeslot

import org.powertac.common.Orderbook
import org.powertac.common.ClearedTrade
import org.powertac.common.interfaces.BrokerProxy
import org.powertac.common.PluginConfig
import org.powertac.common.interfaces.CompetitionControl
import org.joda.time.Instant

import org.powertac.common.OrderbookBid
import org.powertac.common.OrderbookAsk

/**
 * Implementation of {@link org.powertac.common.interfaces.Auctioneer}
 *
 * @author Carsten Block, Daniel Schnurr
 */

class AuctionService implements Auctioneer,
    org.powertac.common.interfaces.BrokerMessageListener,
      org.powertac.common.interfaces.TimeslotPhaseProcessor 
{
  public static final AscPriceShoutComparator = [compare: {Shout a, Shout b -> a.limitPrice.equals(b.limitPrice) ? 0 : a.limitPrice < b.limitPrice ? -1 : 1}] as Comparator
  public static final DescPriceShoutComparator = [compare: {Shout a, Shout b -> a.limitPrice.equals(b.limitPrice) ? 0 : a.limitPrice < b.limitPrice ? 1 : -1}] as Comparator
  public static final DescTurnoverComparator = [compare: {Turnover a, Turnover b -> a.executableVolume.equals(b.executableVolume) ? (a.price < b.price ? 1 : -1) : a.executableVolume < b.executableVolume ? 1 : -1}] as Comparator
  
  static transactional = true

  def timeService
  def accountingService
  BrokerProxy brokerProxyService
  CompetitionControl competitionControlService

  // read this from plugin config
  PluginConfig configuration
  int simulationPhase = 2
  
  // queue of incoming messages - should all be shouts
  List incoming = []
  Object incomingLock = new Object()

  /**
   * Register for phase 2 activation, to drive wholesale market funcitonality
   */
  void init() {
    competitionControlService?.registerTimeslotPhase(this, simulationPhase)
    brokerProxyService?.registerBrokerMarketListener(this)
  }

  // ----------------- Broker message API --------------------
  /**
   * Receives incoming broker message
   */
  @Override
  public void receiveMessage(msg) {
    // dispatch incoming message
    if (msg instanceof Shout) {
      log.debug "Processing incoming shout from BrokerProxy: ${msg}"
      //processShout(msg)
      synchronized (incomingLock) {
        incoming << msg
      }
      //if we need a ACK message: brokerProxyService.sendMessage(msg.broker, "ACK for Shout msg")
    } else {
      brokerProxyService.sendMessage(msg.broker, "No valid object")
    }
  }

  @Override
  public void activate(Instant time, int phase) {
    log.debug "Activate() called: clearing market now."
    while (incoming.size() > 0) {
      // we minimize the time spent in the lock by just grabbing the message
      // and processing it after we release the lock
      def thing
      synchronized (incomingLock) {
        thing = incoming.first()
        incoming = incoming.tail()
      }
      processShout(thing)
    }
    clearMarket()
    cleanOrderbooks()
  }

  /*
  * Implement Auctioneer interface methods
  */

  /**
   * Process incoming shout: validate submitted shout and add to market's orderbook.
   * Add serverside properties: modReasonCode, transactionId, (comment)s
   * Update the orderbook and persist shout        */
  void processShout(Shout shout) {

    /** check if incoming shout is valid (timeslot, product)   */
    if (shout.timeslot.enabled && shout.product == ProductType.Future && 
        shout.quantity && shout.limitPrice) {
      //Shout shout = new Shout(incomingShout.properties)
      shout.transactionId = IdGenerator.createId()
      shout.modReasonCode = ModReasonCode.INSERT

      /** persist incoming shout with market annotations (transactionId / modReasonCode)   */
      if (!shout.save()) {
        log.error("Failed to create shout: ${shout.errors}")
        log.error shout.errors.each { it.toString() }
      } else {
        log.debug "Successfully processed shout: ${shout}"
      }

      /** updated the orderbook   */
      updateOrderbook(shout)

    } else {
      /** hook to implement feedback for brokers concerning invalid shouts   */
      log.info("Incoming shout was invalid: (${shout.broker.username}, ${shout.timeslot}, ${shout.product}, ${shout.buySellIndicator}, ${shout.quantity}, ${shout.limitPrice})")
      if (!shout.limitPrice) log.error("Market order type is not supported in this version.")
    }

  }


  void clearMarket() {

    log.debug "Starting to clear the market"

    def products = ProductType.values()
    def timeslots = Timeslot.findAllByEnabled(true)
    def clearedTradeList = []
    def orderbookList = []

    /** find and process all shout candidates for each enabled timeslot and each product       */
    timeslots.each { timeslot ->
      products.each { product ->
        Turnover turnover
        log.debug "Starting to clear product: ${product} for timeslot ${timeslot}"
        /** set unique transactionId for clearing this particular timeslot and product        */
        String transactionId = IdGenerator.createId()

        /** take snapshot of orderbook before matching and append it to orderbookList    */
        Orderbook ob = Orderbook.findByTimeslotAndProduct(timeslot, product)
        if (!ob) {
          ob = new Orderbook(timeslot: timeslot, product: product, dateExecuted: timeService.currentTime)
          timeslot.addToOrderbooks(ob)
        }
        ob.transactionId = transactionId
        if (!ob.save()) log.error "Failed to save Orderbook with clearing-transactionId: ${ob.errors} "

        /** find candidates that have to be cleared for this timeslot       */
        def candidates = Shout.withCriteria {
          eq('product', product)
          eq('timeslot', timeslot)
          eq('modReasonCode', ModReasonCode.INSERT)
        }

        log.debug "size1 ${candidates.size()}"

        /** calculate uniform execution price for following clearing       */
        if (candidates?.size() >= 1) {
          turnover = calcUniformPrice(candidates)
        }
        else {
          log.info "No Shouts found for uniform price calculation."
        }

        /** split candidates list in sorte bid and ask lists       */
        List bids = candidates.findAll {it.buySellIndicator == BuySellIndicator.BUY}.sort(DescPriceShoutComparator)
        List asks = candidates.findAll {it.buySellIndicator == BuySellIndicator.SELL}.sort(AscPriceShoutComparator)

        if (!turnover?.executableVolume || !turnover?.price)
          log.info("Turnover did not contain information on executable volume and / or price for product ${product} and timeslot ${timeslot}.")
        log.debug "Price for product ${product} and timeslot ${timeslot}: ${turnover?.price}"

        if (candidates?.size() < 1) {
          log.info "No Shouts found for allocation."
        } else {
          /** Determine bids (asks) that are above (below) the determined execution price       */
          bids = bids.findAll {it.limitPrice >= turnover.price}
          asks = asks.findAll {it.limitPrice <= turnover.price}

          BigDecimal aggregQuantityBid = 0.0
          BigDecimal aggregQuantityAsk = 0.0

          /** Allocate all single bids equal/above the execution price       */
          Iterator bidIterator = bids.iterator()
          while (bidIterator.hasNext() && aggregQuantityBid < turnover.executableVolume) {
            aggregQuantityBid = allocateSingleShout(bidIterator.next(), aggregQuantityBid, turnover, transactionId)
          }

          /** Allocate all single asks equal/below the execution price       */
          Iterator askIterator = asks.iterator()
          while (askIterator.hasNext() && aggregQuantityAsk < turnover.executableVolume) {
            aggregQuantityAsk = allocateSingleShout(askIterator.next(), aggregQuantityAsk, turnover, transactionId)
          }

          /** matched quantity of bids must equal matched quantity of asks     */
          if (aggregQuantityAsk != aggregQuantityBid) log.error "Clearing: aggregQuantityAsk does not equal aggregQuantityBid for ${timeslot} and product ${product}"

          /** create clearedTrade instance to save public information about particular clearing and append it to clearedTradeList     */
          if (turnover?.executableVolume && turnover?.price) {

            ClearedTrade ct = new ClearedTrade( timeslot: timeslot,
                                                product: product,
                                                executionPrice: turnover.price,
                                                executionQuantity: turnover.executableVolume,
                                                transactionId: transactionId,
                                                dateExecuted: timeService.currentTime)

            if (!ct.save()) log.error "Failed to save ClearedTrade: ${ct.errors}"
            clearedTradeList << ct
            ob.clearingPrice = turnover.price
            if (!ob.save()) log.error "Failed to save Orderbook with clearingPrice after matching: ${ob.errors} "
          }

          orderbookList << ob

          /** find unmatched shouts that have to be cancelled     */
          def remaining = Shout.withCriteria {
            eq('product', product)
            eq('timeslot', timeslot)
            eq('modReasonCode', ModReasonCode.INSERT)
          }
          if (remaining) {
            remaining.each {shout ->
              shout.modReasonCode = ModReasonCode.DELETIONBYSYSTEM
              if (!shout.save()) log.error "Failed to save cancelled unmatched shout ${shout}"
            }
          }
        }
      }
    }

    reportPublicInformation(clearedTradeList)
    reportPublicInformation(orderbookList)

    log.debug "Ended clearing process in the wholesale market"
  }

  /**
   * Calculate uniform price for the current market clearing. The price is determined in order to maximize
   * the execution quantity. If there exist more than one price, the price with the minimum surplus is chosen.
   *
   * @param shouts - list of all potential candidates for the matching per product in a specified timeslot
   *
   * @return Turnover that contains data of the determined values (price, executableVolume, ...)
   */
  public Turnover calcUniformPrice(List<Shout> shouts) {

    log.debug("Pricing shouts with uniform pricing...");
    def prices = shouts.collect {it.limitPrice}.unique()

    /** Iterate over all submitted limit prices in order to find price that maximizes execution volume.
     *  Order turnovers of different prices in SortedSet turnovers
     *  Stop if turnover is decreasing between two iterations
     */
    Boolean maxTurnoverFound = false;
    Iterator itr = prices.iterator()
    Turnover minEquilibriumTurnover
    Turnover maxEquilibriumTurnover

    while (itr.hasNext() && !maxTurnoverFound) {
      Turnover turnover = new Turnover()
      BigDecimal price = (BigDecimal) itr.next()
      def matchingBids = shouts.findAll {it.buySellIndicator == BuySellIndicator.BUY && it.limitPrice >= price}
      def matchingAsks = shouts.findAll {it.buySellIndicator == BuySellIndicator.SELL && it.limitPrice <= price}
      turnover.aggregatedQuantityBid = matchingBids?.size() > 0 ? (BigDecimal) matchingBids.sum {it.quantity} : 0.0
      turnover.aggregatedQuantityAsk = matchingAsks?.size() > 0 ? (BigDecimal) matchingAsks.sum {it.quantity} : 0.0
      turnover.price = price

      /** store minimum and maximum price each time volume increases, set maximum each time volume is equal*/
      //if (!maxEquilibriumTurnover) { maxEquilibriumTurnover = turnover }

      if (!maxEquilibriumTurnover || maxEquilibriumTurnover <= turnover) {
        if (maxEquilibriumTurnover?.equals(turnover)) {
          maxEquilibriumTurnover = turnover
        } else {
          minEquilibriumTurnover = turnover
          maxEquilibriumTurnover = turnover
        }
      } else {
        maxTurnoverFound = true
      }
    }

    /** Turnover implement comparable interface and are sorted according to max executable volume */
    if (maxEquilibriumTurnover) {
      Turnover clearingTurnover = new Turnover()
      clearingTurnover.price = (maxEquilibriumTurnover.price + minEquilibriumTurnover.price)/2
      clearingTurnover.aggregatedQuantityAsk = Math.min(maxEquilibriumTurnover.aggregatedQuantityAsk, minEquilibriumTurnover?.aggregatedQuantityAsk)
      clearingTurnover.aggregatedQuantityBid = Math.min(maxEquilibriumTurnover.aggregatedQuantityBid, minEquilibriumTurnover?.aggregatedQuantityBid)
      return clearingTurnover
    } else {
      log.debug "No maximum turnover found during uniform price calculation"
      return null
    }

  }

  /**
   * Allocate a single shout by updating modReasonCode(EXECUTION/PARTIALEXECUTION).
   * Set execution price, execution quantity, transactionId and comment.
   *
   * @param shout - incoming shout to allocate, i.e. to set allocation price, allocation quantity and so on
   * @param aggregQuantityAllocated - overall quantity that was previously allocated to other shouts
   * @param turnover - turnover data that contains <code>executableVolume</code> and <code>price</code>
   * @param transactionId - unique id that was set for this clearing of the timeslot/product
   *
   * @return aggregQuantityAllocated  updated allocated overall quantity (aggregQuantityAllocated_old + executionQuantity)
   */
  private BigDecimal allocateSingleShout(Shout incomingShout, BigDecimal aggregQuantityAllocated, Turnover turnover, String transactionId) {

    BigDecimal executableVolume = turnover.executableVolume
    Shout allocatedShout

    if (!incomingShout.validate()) log.error("Failed to validate shout when allocating single shout: ${incomingShout.errors}")

    if (incomingShout.quantity + aggregQuantityAllocated <= executableVolume) { // shout can be entirely matched
      allocatedShout = incomingShout.initModification(ModReasonCode.EXECUTION)
      allocatedShout.executionQuantity = incomingShout.quantity
      allocatedShout.quantity = 0.0
    } else if (executableVolume - aggregQuantityAllocated > 0) {  // shout can only be partially matched
      allocatedShout = incomingShout.initModification(ModReasonCode.PARTIALEXECUTION)
      allocatedShout.executionQuantity = executableVolume - aggregQuantityAllocated
      allocatedShout.quantity = incomingShout.quantity - allocatedShout.executionQuantity
    } else {
      log.error("Market could not be cleared. Unexpected conditions when allocating single shout")
    }
    allocatedShout.executionPrice = turnover.price
    allocatedShout.transactionId = transactionId
    allocatedShout.comment = "Matched by org.powertac.auctioneer.pda"
    if (!allocatedShout.save()) "Failed to save allocated Shout: ${allocatedShout.errors}"

    /** Settlement: reporting market transaction to accountingService        */
    def settlementQuantity = (allocatedShout.buySellIndicator == BuySellIndicator.BUY) ? allocatedShout.executionQuantity : -allocatedShout.executionQuantity
    def settlementPrice = (allocatedShout.buySellIndicator == BuySellIndicator.BUY) ? allocatedShout.executionPrice : -allocatedShout.executionPrice
    log.debug "allocate ${allocatedShout.buySellIndicator} qty ${settlementQuantity} at ${settlementPrice}, ts ${allocatedShout.timeslot.serialNumber}"
    accountingService.addMarketTransaction(allocatedShout.broker, allocatedShout.timeslot, settlementPrice, settlementQuantity)

    aggregQuantityAllocated += allocatedShout.executionQuantity
    return aggregQuantityAllocated
  }

  /**
   * Send list of public information, i.e. list of clearedTrade or Orderbook instances to BrokerProxy
   *    *
   * @param List < E >  - list of clearedTrade or Orderbook instances containing all public information about latest clearing
   */
  private void reportPublicInformation(List publicInformation) {
    brokerProxyService?.broadcastMessages(publicInformation)
  }

  /*
  * Update the orderbook after processing a shout.
  *
  * @param shout - processed Shout that may lead to orderbook update
  *
  * @return ob - updated orderbook that holds bids/asks and corresponding quantities
  */

  private Orderbook updateOrderbook(Shout shout) 
  {
    /** check if there is an existing orderbook else create new one    */
    Orderbook ob = (Orderbook) Orderbook.withCriteria(uniqueResult: true) {
      eq('product', shout.product)
      eq('timeslot', shout.timeslot)
    }

    if (!ob) {
      ob = new Orderbook()  //create new orderbook
      ob.product = shout.product
      ob.timeslot = shout.timeslot
      ob.transactionId = shout.transactionId
      ob.dateExecuted = timeService.currentTime
    }

    def oe

    if (shout.buySellIndicator == BuySellIndicator.BUY) {
      /** check if price level does already exist in orderbook else initialize new OrderbookBid    */
      oe = ob.bids?.find {it -> it.limitPrice == shout.limitPrice}
      if (!oe) { 
        oe = new OrderbookBid(limitPrice: shout.limitPrice, quantity: 0)
      }
      oe.quantity += shout.quantity
      ob.addToBids(oe)
    } 
    else {
      /** check if price level does already exist in orderbook else initialize new OrderbookAsk    */
      oe = ob.asks?.find {it -> it.limitPrice == shout.limitPrice}
      if (!oe) {
        oe = new OrderbookAsk(limitPrice: shout.limitPrice, quantity: 0)
      }
      oe.quantity += shout.quantity
      ob.addToAsks(oe)

    }

    ob.timeslot.addToOrderbooks(ob)
    /*
    if (oe.buySellIndicator == BuySellIndicator.BUY) {

    } else {
      if (oe.buySellIndicator == BuySellIndicator.SELL) {}
    } */

    if (!ob.save()) {
      log.error "Failed to save orderbook: ${ob.errors}"
    } 
    else {
      log.debug "Succesfully saved orderbook: ${ob}"
    }
    ob.timeslot.save()
    return ob
  }
  
  /**
   * Removes cleared bids and asks from the orderbook after clearing
   * and before new bids and asks arrive. This must be called within
   * the same transaction as the call to clearMarket()
   */
  private void cleanOrderbooks ()
  {
    def timeslots = Timeslot.findAllByEnabled(true)
    timeslots.each { timeslot ->
      def orderbooks = Orderbook.findAllByTimeslot(timeslot)
      orderbooks.each { ob ->
        ob.bids.each { bid -> bid.delete() }
        ob.bids.clear()
        ob.asks.each { ask -> ask.delete() }
        ob.asks.clear()
        if (!ob.save()) {
          log.error "Failed to save orderbook: ${ob.errors}"
        }
      }      
    }
  }
}
