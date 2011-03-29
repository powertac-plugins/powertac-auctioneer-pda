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
import org.powertac.common.Competition
import org.powertac.common.enumerations.ModReasonCode
import org.powertac.common.IdGenerator
import org.powertac.common.exceptions.ShoutCreationException
import org.powertac.common.exceptions.ShoutDeletionException
import org.powertac.common.Product
import org.powertac.common.enumerations.BuySellIndicator
import org.powertac.common.MarketTransaction
import org.powertac.common.enumerations.MarketTransactionType
import org.powertac.common.enumerations.OrderType
import org.powertac.common.Timeslot
import org.powertac.common.exceptions.ShoutUpdateException
import org.powertac.common.Orderbook

/**
 * Implementation of {@link org.powertac.common.interfaces.Auctioneer}
 *
 * @author Carsten Block, Daniel Schnurr
 */

class AuctionService implements Auctioneer {

  public static final AscPriceShoutComparator = [compare: {Shout a, Shout b -> a.limitPrice.equals(b.limitPrice) ? 0 : a.limitPrice < b.limitPrice ? -1 : 1}] as Comparator
  public static final DescPriceShoutComparator = [compare: {Shout a, Shout b -> a.limitPrice.equals(b.limitPrice) ? 0 : a.limitPrice < b.limitPrice ? 1 : -1}] as Comparator

  def accountingService
  /*
  * Implement Auctioneer interface methods
  */

  /**
   * Process incoming shout: validate submitted shout and add to market's orderbook
   * Add serverside properties: modReasonCode, transactionId, (comment)s
   * Update the orderbook and persist shout    */
  void processShout(Shout incomingShout) {
    if (!incomingShout.limitPrice) log.error("Market order type is not supported in this version.")

    incomingShout.transactionId = IdGenerator.createId()
    incomingShout.modReasonCode = ModReasonCode.INSERT


    if (!incomingShout.save()) {
      log.error("Failed to create shout: ${incomingShout.errors}")
      log.error incomingShout.errors.each { it.toString() }
    }

    Orderbook updatedOrderbook = updateOrderbook(incomingShout)
    if (updatedOrderbook) {
      if (!updatedOrderbook.save()) {log.error "Failed to save orderbook: ${incomingShout.errors}"}
    }
  }


  void clearMarket() {

    def products = Product.findAll()
    def timeslots = Timeslot.findAllByEnabled(true)
    Turnover turnover

    /** find and process all shout candidates for each enabled timeslot and each product   */
    timeslots.each { timeslot ->
      products.each { product ->

        /** set unique transactionId for clearing this particular timeslot and product    */
        String transactionId = IdGenerator.createId()

        /** find candidates that have to be cleared for this timeslot   */
        def candidates = Shout.withCriteria {
          eq('product', product)
          eq('timeslot', timeslot)
        }

        /** calculate uniform execution price for following clearing   */
        if (candidates?.size() >= 1) {
          turnover = calcUniformPrice(candidates)
        } else {
          log.info "No Shouts found for uniform price calculation."
        }

        /** split candidates list in sorte bid and ask lists   */
        List bids = candidates.findAll {it.buySellIndicator == BuySellIndicator.BUY}.sort(DescPriceShoutComparator)
        List asks = candidates.findAll {it.buySellIndicator == BuySellIndicator.SELL}.sort(AscPriceShoutComparator)

        if (!turnover.executableVolume || !turnover.price) log.error("Turnover did not contain information on executable volume and / or price.")

        if (candidates?.size() < 1) {
          log.info "No Shouts found for allocation."
        } else {
          /** Determine bids (asks) that are above (below) the determined execution price   */
          bids = bids.findAll {it.limitPrice >= turnover.price}
          asks = asks.findAll {it.limitPrice <= turnover.price}

          BigDecimal aggregQuantityBid = 0.0
          BigDecimal aggregQuantityAsk = 0.0

          /** Allocate all single bids equal/above the execution price   */
          Iterator bidIterator = bids.iterator()
          while (bidIterator.hasNext() && aggregQuantityBid < turnover.executableVolume) {
            /** Todo implement cash and position settlement   */
            aggregQuantityBid = allocateSingleShout(bidIterator.next(), aggregQuantityBid, turnover, transactionId)
          }

          /** Allocate all single asks equal/below the execution price   */
          Iterator askIterator = asks.iterator()
          while (askIterator.hasNext() && aggregQuantityAsk < turnover.executableVolume) {
            aggregQuantityAsk = allocateSingleShout(askIterator.next(), aggregQuantityAsk, turnover, transactionId)
          }

          /** Todo writeTradeLog(aggregQuantityAsk, aggregQuantityBid), replace stat object in method with turnover object   */
        }
      }
    }

  }

  /**
   * Calculate uniform price for the current market clearing. The price is determined in order to maximize
   * the execution quantity. If there exist more than one price, the price with the minimum surplus is chosen.
   *
   * @param shouts - list of all potential candidates for the matching per product in a specified timeslot
   *
   * @return Map that contains statistical data of the determined values (price, executableVolume, ...)
   */
  public Turnover calcUniformPrice(List<Shout> shouts) {

    log.debug("Pricing shouts with uniform pricing...");

    SortedSet<Turnover> turnovers = new TreeSet<Turnover>()
    def prices = shouts.collect {it.limitPrice}.unique()

    prices.each {price ->
      Turnover turnover = new Turnover()
      def matchingBids = shouts.findAll {it.buySellIndicator == BuySellIndicator.BUY && it.limitPrice >= price}
      def matchingAsks = shouts.findAll {it.buySellIndicator == BuySellIndicator.SELL && it.limitPrice <= price}
      turnover.aggregatedQuantityBid = matchingBids?.size() > 0 ? (BigDecimal) matchingBids.sum {it.quantity} : 0.0
      turnover.aggregatedQuantityAsk = matchingAsks?.size() > 0 ? (BigDecimal) matchingAsks.sum {it.quantity} : 0.0
      turnover.price = (BigDecimal) price
      turnovers << turnover
    }
    Turnover maxTurnover = turnovers?.first() //Turnover implement comparable interface and are sorted according to max executable volume and then min surplus
    if (maxTurnover) {
      //stat.putAll(aggregatedQuantityAsk: maxTurnover.aggregatedQuantityAsk, aggregatedQuantityBid: maxTurnover.aggregatedQuantityBid, price: maxTurnover.price, executableVolume: maxTurnover.executableVolume, surplus: maxTurnover.surplus, pricingStatus: "Success")
      return maxTurnover
    } else {
      log.info "No maximum turnover found during uniform price calculation"
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

    if (incomingShout.quantity + aggregQuantityAllocated <= executableVolume) {
      allocatedShout = incomingShout.initModification(ModReasonCode.EXECUTION)
      allocatedShout.executionQuantity = incomingShout.quantity
      allocatedShout.quantity = 0.0
    } else if (executableVolume - aggregQuantityAllocated > 0) {
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

    /** Todo: Settlement has to be implemented   */
    accountingService.addMarketTransaction(allocatedShout.broker, allocatedShout.timeslot, allocatedShout.executionPrice, allocatedShout.executionQuantity)

    aggregQuantityAllocated += allocatedShout.executionQuantity
    return aggregQuantityAllocated
  }

  /**
   * Create and save updated Quote as a TransactionLog instance for the given *sorted* list of asks and bids.
   * Previous Quote with latest=true of product and timeslot is set outdated and persisted.
   *
   * @param bids - sorted by descending limit price
   * @param asks - sorted by ascending limit price
   * @param stat - statistics data contain information about product, timeslot and transactionId
   *
   * @return TransactionLog object with quote data (ask, bid, askSize, bidSize) for specified product and timeslot
   */
  /* Todo Reporting Trade Functionality has to be reimplemented
  private MarketTransaction writeTradeLog(Map stat) {
    MarketTransaction tl = (MarketTransaction) MarketTransaction.withCriteria(uniqueResult: true) {
      eq('product', stat.product)
      eq('timeslot', stat.timeslot)
      eq('transactionType', MarketTransactionType.TRADE)
    }

    //if (oldTl) {
    //  oldTl.latest = false
    //  if (!oldTl.save()) log.error("Failed to save outdated TransactionLog after clearing: ${oldTl.errors}")
    //}

    if (tl == null)
      tl = new MarketTransaction()
    tl.transactionType = MarketTransactionType.TRADE
    tl.product = stat.product
    tl.timeslot = stat.timeslot
    tl.transactionId = stat.transactionId

    tl.price = stat.price
    tl.quantity = stat.executableVolume

    if (!tl.save()) log.error("Failed to save TransactionLog after clearing: ${tl.errors}")
    return tl
  } */

  /*
  * Update the orderbook after processing a shout.
  * Get top ten bids and asks from database and update the Orderbook object.
  *
  * @param shout - processed Shout that may lead to orderbook update
  *
  * @return latestOrderbook - updated orderbook that holds the top ten bids/asks and corresponding quantities
  */

  private Orderbook updateOrderbook(Shout shout) {

    Boolean firstOrderbook

    Orderbook latestOrderbook = (Orderbook) Orderbook.withCriteria(uniqueResult: true) {
      eq('product', shout.product)
      eq('timeslot', shout.timeslot)
    }

    if (!latestOrderbook) {
      latestOrderbook = new Orderbook()  //create new orderbook
      firstOrderbook = true
    }

    def bestAsks = Shout.withCriteria() {
      eq('product', shout.product)
      eq('timeslot', shout.timeslot)
      eq('buySellIndicator', BuySellIndicator.SELL)
      maxResults(10)
      order('limitPrice', 'asc')

      projections {
        groupProperty('limitPrice')
        sum('quantity')
      }
    }

    def bestBids = Shout.withCriteria() {
      eq('product', shout.product)
      eq('timeslot', shout.timeslot)
      eq('buySellIndicator', BuySellIndicator.BUY)
      maxResults(10)
      order('limitPrice', 'desc')

      projections {
        groupProperty('limitPrice')
        sum('quantity')
      }
    }

    BigDecimal[][][] newOrderbookArray = new BigDecimal[2][2][10]
    BigDecimal[][][] latestOrderbookArray = latestOrderbook.getOrderbookArray()
    Boolean orderbookChangeFound = false
    Integer levelCounter = 0

    if (bestBids.size() == 0) { //no open bid orders left
      if (latestOrderbook.bid0 != null) {
        orderbookChangeFound = true //empty bid orderbook is new situation if latestOrderbook contained bid
      }
    }
    else {
      //BID: check if new orderbook entry && create newOrderbookArray
      levelCounter = 0
      while (levelCounter <= 9) {
        //create newOrderbookArray bid entry
        if (bestBids[levelCounter]) {
          newOrderbookArray[0][0][levelCounter] = bestBids[levelCounter][0] //price
          newOrderbookArray[0][1][levelCounter] = bestBids[levelCounter][1] //size
        } else {
          //fill empty levels (in case of deletion this is necessary)
          newOrderbookArray[0][0][levelCounter] = null
          newOrderbookArray[0][1][levelCounter] = 0.0
        }

        //changed?
        if (!orderbookChangeFound && !(latestOrderbookArray[0][0][levelCounter] == newOrderbookArray[0][0][levelCounter] && latestOrderbookArray[0][1][levelCounter] == newOrderbookArray[0][1][levelCounter])) {
          orderbookChangeFound = true
        }
        levelCounter++
      }
    }

    //no open ask orders left
    if (bestAsks.size() == 0) {
      //empty ask orderbook is new situation
      if (latestOrderbook.ask0 != null) {
        orderbookChangeFound = true
      }
    }
    else {
      //ASK: check if new orderbook entry && create newOrderbookArray
      levelCounter = 0

      while (levelCounter <= 9) {
        //create newOrderbookArray bid entry
        if (bestAsks[levelCounter]) {
          newOrderbookArray[1][0][levelCounter] = bestAsks[levelCounter][0] //price
          newOrderbookArray[1][1][levelCounter] = bestAsks[levelCounter][1] //size
        } else {
          //fill empty levels (in case of deletion this is necessary)
          newOrderbookArray[1][0][levelCounter] = null
          newOrderbookArray[1][1][levelCounter] = 0.0
        }

        //changed?
        if (!orderbookChangeFound && !(latestOrderbookArray[1][0][levelCounter] == newOrderbookArray[1][0][levelCounter] && latestOrderbookArray[1][1][levelCounter] == newOrderbookArray[1][1][levelCounter])) {
          orderbookChangeFound = true
        }
        levelCounter++

      }
    }

    //If there are changes found create new orderbook entry
    if (orderbookChangeFound) {
      if (!firstOrderbook) {
        //latestOrderbook.latest = false
        if (!latestOrderbook.save()) log.error("Failed to save outdated Orderbook:${latestOrderbook.errors}")
      }

      latestOrderbook.product = shout.product
      latestOrderbook.timeslot = shout.timeslot
      latestOrderbook.transactionId = shout.transactionId
      latestOrderbook.dateExecuted = shout.dateMod
      latestOrderbook.setOrderbookArray(newOrderbookArray)
      if (!latestOrderbook.save()) log.error("Failed to save updated Orderbook: ${newOrderbook.errors}")

      return latestOrderbook
    }
    return null
  }

  /**
   * Delete Shout
   * Validate shoutId, shoutInstance
   * Update/save old version of inserted shout and save/return latest version of deleted shout
   * To do: Broker and Competition validation?
   */
  /*
    public List processShoutDelete(ShoutDoDeleteCmd shoutDoDeleteCmd) {
      List output = []
      def shoutId = shoutDoDeleteCmd.shoutId
      if (!shoutId) {
        log.error("Failed to delete shout. No shout id found: ${shoutId}.")
        return output
      }

      def shoutInstance = Shout.findByShoutId(shoutId, true)
      if (!shoutInstance) {
        log.error("Failed to delete shout. No shout found for id: ${shoutId}")
        return output
      }
      Shout delShout = processShoutDelete(shoutInstance)
      output << delShout

      Orderbook updatedOrderbook = updateOrderbook(delShout)
      if (updatedOrderbook) {
        output << updatedOrderbook
        MarketTransaction updatedQuote = updateQuote(updatedOrderbook)
        if (updatedQuote) output << updatedQuote
      }

      return output
    }
  */

  private Shout processShoutDelete(Shout shoutInstance) throws ShoutDeletionException {

    def delShout = shoutInstance.initModification(ModReasonCode.DELETIONBYUSER)
    delShout.transactionId = IdGenerator.createId()
    if (!delShout.save()) throw new ShoutDeletionException("Failed to save latest version of deleted shout: ${shoutInstance.errors}")

    return delShout
  }

  /**
   * Update Shout
   * Validate shoutId, shoutInstance
   * Delete old shout and create copy with modified quantity/limitPrice
   */
  /*
 public List processShoutUpdate(ShoutDoUpdateCmd shoutDoUpdateCmd) {
   List output = []
   def shoutId = shoutDoUpdateCmd.shoutId
   if (!shoutId) {
     log.error("Failed to update shout. No shout id found: ${shoutId}.")
     return output
   }
   def shoutInstance = Shout.findByShoutId(shoutId, true)
   if (!shoutInstance) {
     log.error("Failed to update shout, No shout found for id: ${shoutId}")
     return output
   }

   def delShout = processShoutDelete(shoutInstance)
   Shout updatedShout = delShout.initModification(ModReasonCode.MODIFICATION)

   updatedShout.quantity = shoutDoUpdateCmd.quantity ?: delShout.quantity
   updatedShout.limitPrice = shoutDoUpdateCmd.limitPrice ?: delShout.limitPrice
   updatedShout.transactionId = IdGenerator.createId()

   if (!updatedShout.save()) {
     log.error("Failed to save latet version of updated shout: ${updatedShout.errors}")
     return output
   }

   Orderbook updatedOrderbook = updateOrderbook(updatedShout)
   if (updatedOrderbook) {
     output << updatedOrderbook
     MarketTransaction updatedQuote = updateQuote(updatedOrderbook)
     if (updatedQuote) output << updatedQuote
   }

   return output
 } */


}
