package org.powertac.auctioneer.pda

import org.powertac.common.interfaces.Auctioneer
import org.powertac.common.command.ShoutDoCreateCmd
import org.powertac.common.Shout
import org.powertac.common.command.ShoutDoDeleteCmd
import org.powertac.common.command.ShoutDoUpdateCmd
import org.powertac.common.Competition
import org.powertac.common.enumerations.ModReasonCode
import org.powertac.common.IdGenerator
import org.powertac.common.exceptions.ShoutCreationException
import org.powertac.common.exceptions.ShoutDeletionException
import org.powertac.common.exceptions.ShoutUpdateExeption
import org.powertac.common.Product
import org.powertac.common.enumerations.BuySellIndicator
import org.powertac.common.TransactionLog
import org.powertac.common.exceptions.MarketClearingException
import javax.transaction.Transaction
import org.powertac.common.enumerations.TransactionType
import org.powertac.common.CashUpdate
import org.powertac.common.command.CashDoUpdateCmd
import org.powertac.common.command.PositionDoUpdateCmd
import org.powertac.common.enumerations.OrderType
import org.powertac.common.Timeslot

class AuctionService implements Auctioneer {


  public static final AscPriceShoutComparator = [compare: {Shout a, Shout b -> a.limitPrice.equals(b.limitPrice) ? 0 : a.limitPrice < b.limitPrice ? -1 : 1}] as Comparator
  public static final DescPriceShoutComparator = [compare: {Shout a, Shout b -> a.limitPrice.equals(b.limitPrice) ? 0 : a.limitPrice < b.limitPrice ? 1 : -1}] as Comparator

  /*
  * Implement Auctioneer interface methods
  */

  /**
   * Create Shout
   * Copy properties from incoming ShoutDoCreateCmd to Shout domain class
   * Populate additional properties and save shout
   */
  List processShoutCreate(ShoutDoCreateCmd shoutDoCreate) {

    if (shoutDoCreate.orderType == OrderType.MARKET) throw new ShoutCreationException("Market oder type is not supported in this version.")

    Shout shoutInstance = new Shout()

    shoutInstance.broker = shoutDoCreate.broker
    shoutInstance.competition = shoutDoCreate.competition

    shoutInstance.product = shoutDoCreate.product
    shoutInstance.timeslot = shoutDoCreate.timeslot
    shoutInstance.limitPrice = shoutDoCreate.limitPrice
    shoutInstance.quantity = shoutDoCreate.quantity
    shoutInstance.buySellIndicator = shoutDoCreate.buySellIndicator
    shoutInstance.orderType = shoutDoCreate.orderType

    shoutInstance.modReasonCode = ModReasonCode.INSERT
    shoutInstance.shoutId = IdGenerator.createId()
    shoutInstance.transactionId = shoutInstance.shoutId
    shoutInstance.latest = true

    if (!shoutInstance.save()) throw new ShoutCreationException("Failed to create shout: ${shoutInstance.errors}")

    return null
  }

  /**
   * Delete Shout
   * Validate shoutId, shoutInstance
   * Update/save old version of inserted shout and save/return latest version of deleted shout
   * Todo: Broker and Competition validation?
   */
  public Shout processShoutDelete(ShoutDoDeleteCmd shoutDoDeleteCmd) {

    def shoutId = shoutDoDeleteCmd.shoutId
    if (!shoutId) throw new ShoutDeletionException("Failed to delete shout. No shout id found: ${shoutId}.")

    def shoutInstance = Shout.findByShoutIdAndLatest(shoutId, true)
    if (!shoutInstance) throw new ShoutDeletionException("Failed to delete shout. No shout found for id: ${shoutId}")

    return processShoutDelete(shoutInstance)
  }

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
  public List processShoutUpdate(ShoutDoUpdateCmd shoutDoUpdateCmd) {

    def shoutId = shoutDoUpdateCmd.shoutId
    if (!shoutId) throw new ShoutUpdateExeption("Failed to update shout. No shout id found: ${shoutId}.")

    def shoutInstance = Shout.findByShoutIdAndLatest(shoutId, true)
    if (!shoutInstance) throw new ShoutUpdateExeption("Failed to update shout, No shout found for id: ${shoutId}")


    def delShout = processShoutDelete(shoutInstance)
    Shout updatedShout = delShout.initModification(ModReasonCode.MODIFICATION)

    updatedShout.quantity = shoutDoUpdateCmd.quantity ?: delShout.quantity
    updatedShout.limitPrice = shoutDoUpdateCmd.limitPrice ?: delShout.limitPrice
    updatedShout.transactionId = IdGenerator.createId()

    if (!updatedShout.save()) throw new ShoutUpdateExeption("Failed to save latet version of updated shout: ${updatedShout.errors}")

    return null
  }

  public List clearMarket() {
    List resultingList = []
    def currentCompetition = Competition.findByCurrent(true)
    def products = Product.findAllByCompetition(currentCompetition)
    def timeslots = Timeslot.findAllByEnabled(true)

    // Find and process all shout candidates for each timeslot and each product
    timeslots.each { timeslot ->
      products.each { product ->

        def candidates = Shout.withCriteria {
          eq('product', product)
          eq('timeslot', timeslot)
          eq('latest', true)
        }

        Map stat = calcUniformPrice(candidates)
        stat.product = product
        stat.competition = currentCompetition
        stat.timeslot = timeslot
        stat.transactionId = IdGenerator.createId()

        List bids = candidates.findAll {it.buySellIndicator == BuySellIndicator.BUY}.sort(DescPriceShoutComparator)
        List asks = candidates.findAll {it.buySellIndicator == BuySellIndicator.SELL}.sort(AscPriceShoutComparator)

        resultingList << writeQuote(bids, asks, stat)

        if (!stat.executableVolume || !stat.price) throw new MarketClearingException("Stats did not contain information on executable volume and / or price.")

        if (candidates?.size() < 1) {
          stat.allocationStatus = "No Shouts found for allocation."
        } else {
          //Determine bids and asks that will be allocated in this matching
          bids = bids.findAll {it.limitPrice >= stat.price}
          asks = asks.findAll {it.limitPrice <= stat.price}

          BigDecimal aggregQuantityBid = 0.0
          BigDecimal aggregQuantityAsk = 0.0

          //Allocate bids
          Iterator bidIterator = bids.iterator()
          while (bidIterator.hasNext() && aggregQuantityBid < stat.executableVolume) {
            List results = allocateSingleShout(bidIterator.next(), aggregQuantityBid, stat) //results contains processed shout (index 0) and updated aggregQuantityBid (index 1)
            resultingList << results[0] //add allocated Shout to returned list
            resultingList << settleCashUpdate(results[0]) //add CashDoUpdateCmd to returned list
            resultingList << settlePositionUpdate(results[0])  //add PositionDoUpdateCmd to returned list
            aggregQuantityBid = results[1]
          }
          //Allocate asks
          Iterator askIterator = asks.iterator()
          while (askIterator.hasNext() && aggregQuantityAsk < stat.executableVolume) {
            List results = allocateSingleShout(askIterator.next(), aggregQuantityAsk, stat) //results contains processed shout (index 0) and updated aggregQuantityAsk (index 1)
            resultingList << results[0] //add allocated Shout to returned list
            resultingList << settleCashUpdate(results[0]) //add CashDoUpdateCmd to returned list
            resultingList << settlePositionUpdate(results[0])  //add PositionDoUpdateCmd to returned list
            aggregQuantityBid = results[1]
          }

          stat.allocatedQuantityAsk = aggregQuantityAsk
          stat.allocatedQuantityBid = aggregQuantityBid
        }
      }
    }

    return resultingList
  }

  /**
   * Calculate uniform price for the current market clearing. The price is determined in order to maximize
   * the execution quantity. If there exist more than one price, the price with the minimum surplus is chosen.
   *
   * @param shouts - list of all potential candidates for the matching per product in a specified timeslot
   *
   * @return Map that contains statistical data of the determined values (price, executableVolume, ...)
   */
  public Map calcUniformPrice(List<Shout> shouts) {
    Map stat = [:]
    log.debug("Pricing shouts with uniform pricing...");

    if (shouts?.size() < 1) {
      stat.pricingStatus = "No Shouts found for uniform price calculation."
      return stat
    } else {
      SortedSet<Turnover> turnovers = new TreeSet<Turnover>()
      def prices = shouts.collect {it.limitPrice}.unique()
      prices.each {price ->
        Turnover turnover = new Turnover()
        def matchingBids = shouts.findAll {it.buySellIndicator == BuySellIndicator.BUY && it.limitPrice >= price}
        def matchingAsks = shouts.findAll {it.buySellIndicator == BuySellIndicator.SELL && it.limitPrice <= price}
        turnover.aggregatedQuantityBid = matchingBids?.size() > 0 ? matchingBids.sum {it.quantity} : 0.0
        turnover.aggregatedQuantityAsk = matchingAsks?.size() > 0 ? matchingAsks.sum {it.quantity} : 0.0
        turnover.price = (BigDecimal) price
        turnovers << turnover
      }
      Turnover maxTurnover = turnovers?.first() //Turnover implement comparable interface and are sorted according to max executable volume and then min surplus
      if (maxTurnover) {
        stat.putAll(aggregatedQuantityAsk: maxTurnover.aggregatedQuantityAsk, aggregatedQuantityBid: maxTurnover.aggregatedQuantityBid, price: maxTurnover.price, executableVolume: maxTurnover.executableVolume, surplus: maxTurnover.surplus, pricingStatus: "Success")
        return stat
      } else {
        stat?.pricingStatus = "No maximum turnover found during uniform price calculation"
        return stat
      }
    }
  }

  /**
   * Allocate a single shout by updating modReasonCode(EXECUTION/PARTIALEXECUTION) and quantity.
   * Set execution price, execution quantity, transactionId and comment.
   *
   * @param shout - incoming shout to allocate, i.e. to set allocation price, allocation quantity and so on
   * @param aggregQuantityAllocated - overall quantity that was previously allocated to other shouts
   * @param stat - statistics data that has to contain map entries <code>executableVolume</code> and <code>price</code>
   *
   * @return list that contains updated allocatedShout (index 0) and updated aggregQuantityAllocated (index 1)
   */
  private List allocateSingleShout(Shout incomingShout, BigDecimal aggregQuantityAllocated, Map stat) throws MarketClearingException {

    BigDecimal executableVolume = stat.executableVolume
    BigDecimal price = stat.price
    Shout allocatedShout

    if (!incomingShout.validate()) throw new MarketClearingException("Failed to validate shout when allocating single shout: ${incomingShout.errors}")

    if (incomingShout.quantity + aggregQuantityAllocated <= executableVolume) {
      allocatedShout = incomingShout.initModification(ModReasonCode.EXECUTION)
      allocatedShout.executionQuantity = incomingShout.quantity
      allocatedShout.quantity = 0.0
    } else if (executableVolume - aggregQuantityAllocated > 0) {
      allocatedShout = incomingShout.initModification(ModReasonCode.PARTIALEXECUTION)
      allocatedShout.executionQuantity = executableVolume - aggregQuantityAllocated
      allocatedShout.quantity = incomingShout.quantity - allocatedShout.executionQuantity
    } else {
      throw new MarketClearingException("Market could not be cleared. Unexpected conditions when allocating single shout")
    }
    allocatedShout.executionPrice = price
    allocatedShout.transactionId = stat.transactionId
    allocatedShout.comment = "Matched by org.powertac.auctioneer.pda"
    allocatedShout.save()

    aggregQuantityAllocated += allocatedShout.executionQuantity
    return [allocatedShout, aggregQuantityAllocated]
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
  private TransactionLog writeQuote(List bids, List asks, Map stat) throws MarketClearingException {

    TransactionLog oldTl = (TransactionLog) TransactionLog.withCriteria(uniqueResult: true) {
      eq('product', stat.product)
      eq('timeslot', stat.timeslot)
      eq('transactionType', TransactionType.QUOTE)
      eq('latest', true)
    }

    if (oldTl) {
      oldTl.latest = false
      if (!oldTl.save()) throw new MarketClearingException("Failed to save outdated Quote before clearing: ${oldTl.errors}")
    }

    TransactionLog tl = new TransactionLog()
    tl.transactionType = TransactionType.QUOTE
    tl.competition = stat.competition
    tl.product = stat.product
    tl.timeslot = stat.timeslot
    tl.transactionId = stat.transactionId
    tl.latest = true

    if (bids) {
      tl.bid = bids[0].limitPrice
      tl.bidSize = bids[0].quantity
    }
    if (asks) {
      tl.ask = asks[0].limitPrice
      tl.askSize = asks[0].quantity
    }

    if (!tl.save()) throw new MarketClearingException("Failed to save Quote before clearing: ${tl.errors}")
    return tl
  }

  /**
   * Settlement methods calculate position and cashUpdates based on the current market clearing
   * @param shout - matched shout in the current clearing
   *
   * @return cashDoUpdateCommand / positionDoUpdateCommand       *
   */
  private CashDoUpdateCmd settleCashUpdate(Shout shout) {
    CashDoUpdateCmd cashUpdate = new CashDoUpdateCmd()

    cashUpdate.broker = shout.broker
    cashUpdate.competition = shout.competition
    cashUpdate.relativeChange = (shout.buySellIndicator == BuySellIndicator.SELL) ? shout.executionPrice * shout.executionQuantity : -shout.executionPrice * shout.executionQuantity
    cashUpdate.reason = "Clearing ${shout.transactionId} for timeslot ${shout.timeslot} and product ${shout.product}."
    cashUpdate.origin = "org.powertac.auctioneer.pda"

    return cashUpdate
  }

  private PositionDoUpdateCmd settlePositionUpdate(Shout shout) {
    PositionDoUpdateCmd posUpdate = new PositionDoUpdateCmd()

    posUpdate.broker = shout.broker
    posUpdate.competition = shout.competition
    posUpdate.relativeChange = (shout.buySellIndicator == BuySellIndicator.SELL) ? -shout.executionQuantity : shout.executionQuantity
    posUpdate.reason = "Clearing ${shout.transactionId} for timeslot ${shout.timeslot} and product ${shout.product}."
    posUpdate.origin = "org.powertac.auctioneer.pda"

    return posUpdate
  }
/*
* Implement CompetitionBaseEvents interface methods
*/

  void competitionBeforeStart(Competition competition) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  void competitionAfterStart(Competition competition) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  void competitionBeforeStop(Competition competition) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  void competitionAfterStop(Competition competition) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  void competitionReset(Competition competition) {
    //To change body of implemented methods use File | Settings | File Templates.
  }


}
