/*
 * Copyright 2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS,  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.powertac.auctioneer.pda

import org.powertac.common.interfaces.InitializationService
import org.powertac.common.PluginConfig
import org.powertac.common.Competition

class AuctionInitializationService implements InitializationService {

  static transactional = true

  def auctionService

  @Override
  public void setDefaults ()
  {
    PluginConfig auctioneer = new PluginConfig(roleName: 'Auctioneer', configuration: [:])
    auctioneer.save()
  }

  @Override
  public String initialize (Competition competition, List<String> completedInits)
  {
    PluginConfig auctioneer = PluginConfig.findByRoleName('Auctioneer')
    if (!auctioneer) {
      log.error "PluginConfig for Auctioneer does not exist."
    }
    else {
      auctionService.configuration = auctioneer
      return 'Auctioneer'
    }
    return 'fail'
  }
}
