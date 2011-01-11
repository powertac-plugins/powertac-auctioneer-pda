package org.powertac.auctioneer.pda

import org.powertac.common.interfaces.Auctioneer
import org.powertac.common.command.ShoutDoCreateCmd
import org.powertac.common.Shout
import org.powertac.common.command.ShoutDoDeleteCmd
import org.powertac.common.command.ShoutDoUpdateCmd
import org.powertac.common.Competition

class AuctionService implements Auctioneer {

  /*
   * Implement Auctioneer interface methods
   */

  List processShoutCreate(ShoutDoCreateCmd shoutDoCreate) {
    return null  //To change body of implemented methods use File | Settings | File Templates.
  }

  Shout processShoutDelete(ShoutDoDeleteCmd shoutDoDeleteCmd) {
    return null  //To change body of implemented methods use File | Settings | File Templates.
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
