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

class AuctionService implements Auctioneer {


  public static final AscPriceShoutComparator = [compare: {Shout a, Shout b -> a.limitPrice.equals(b.limitPrice) ? 0 : a.limitPrice < b.limitPrice ? -1 : 1}] as Comparator
  public static final DescPriceShoutComparator = [compare: {Shout a, Shout b -> a.limitPrice.equals(b.limitPrice) ? 0 : a.limitPrice < b.limitPrice ? 1 : -1}] as Comparator



  /*
   * Implement Auctioneer interface methods
   */

  /* CREATE SHOUT
   * Copy properties from incoming ShoutDoCreateCmd to Shout domain class
   * Populate additional properties
   * Save shout
   */

  List processShoutCreate(ShoutDoCreateCmd shoutDoCreate) {

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

  /* DELETE SHOUT
   * Validate shoutId, shoutInstance
   * Update/save old version of inserted shout and save/return latest version of deleted shout
   * Todo: Broker and Competition validation?
   */

  Shout processShoutDelete(ShoutDoDeleteCmd shoutDoDeleteCmd) {

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

  /* UPDATE SHOUT
   * Validate shoutId, shoutInstance
   * Delete old shout and create copy with modified quantity/limitPrice
   *
   */

  List processShoutUpdate(ShoutDoUpdateCmd shoutDoUpdateCmd) {

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

  List clearMarket() {

    /*
     * ClearMarket for specific product
     * Identify price in order to execute maximum quantity of shouts
     * Specific or uniform price?
     */
    //Todo: Do for all products and all timeslots
    def product
    def timeslot

    def candidates = Shout.withCriteria {
      eq('product', product)
      eq('timeslot', timeslot)
      eq('latest', true)
      order("limitPrice", "asc")
    }
    Map stat = calcUniformPrice(candidates)




    return null
  }



  private Map calcUniformPrice(List<Shout> shouts) {
    Map stat = [:]
    log.debug("Pricing shouts with uniform pricing...");

    if (shouts?.size() < 1) {
      stat.pricingStatus = "No Shouts found for uniform price calculation."
      return stat
    } else {
      SortedSet turnovers = new TreeSet()
      def prices = shouts.collect {it.limitPrice}.unique()
      prices.each {price ->
        def turnover = new Turnover()
        def matchingBids = shouts.findAll {it.buySellIndicator == BuySellIndicator.BUY && it.limitPrice >= price}
        def matchingAsks = shouts.findAll {it.buySellIndicator == BuySellIndicator.SELL && it.limitPrice <= price}
        turnover.aggregatedQuantityBid = matchingBids?.size() > 0 ? matchingBids.sum {it.quantity} : 0.0
        turnover.aggregatedQuantityAsk = matchingAsks?.size() > 0 ? matchingAsks.sum {it.quantity} : 0.0
        turnover.price = price
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
