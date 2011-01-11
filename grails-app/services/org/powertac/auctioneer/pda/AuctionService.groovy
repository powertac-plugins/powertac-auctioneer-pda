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

class AuctionService implements Auctioneer {

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
    if (!shoutId) throw new ShoutDeletionException("Failed to delete shout: No shout id found: ${shoutId}.")

    def shoutInstance = Shout.findByShoutIdAndLatest(shoutId, true)
    if (!shoutInstance) throw new ShoutDeletionException("Failed to delete shout: No shout found for id: ${shoutId}")

    shoutInstance.setLatest(false)
    if (!shoutInstance.save()) throw new ShoutDeletionException("Failed to save outdated version of deleted shout: ${shoutInstance.errors}")

    def delShout = shoutInstance.initModification(ModReasonCode.DELETIONBYUSER)
    delShout.transactionId = IdGenerator.createId()
    if (!delShout.save()) throw new ShoutDeletionException("Failed to save latest version of deleted shout: ${shoutInstance.errors}")

    return delShout
  }

  List processShoutUpdate(ShoutDoUpdateCmd shoutDoUpdateCmd) {
    return null  //To change body of implemented methods use File | Settings | File Templates.
  }

  List clearMarket() {
    return null  //To change body of implemented methods use File | Settings | File Templates.
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
