class PowertacAuctioneerPdaGrailsPlugin {
    // the plugin version
    def version = "0.2"
    // the version or versions of Grails the plugin is designed for
    def grailsVersion = "1.3.7 > *"
    // the other plugins this plugin depends on
    def dependsOn = ['powertacCommon':'0.10 > *',
                     'powertacServerInterface':'0.2 > *',
                     'powertacAccountingService': '0.4 > *']
    
    // resources that are excluded from plugin packaging
    def pluginExcludes = [
            "grails-app/views/error.gsp"
    ]


    def author = "Daniel Schnurr, Carsten Block"
    def authorEmail = "daniel.schnurr@kit.edu, powertac@carstenblock.org"
    def title = "Call-Market Auctioneer"
    def description = '''\\
This plugin provides a  call-market auctioneer that clears the market upon a triggered clearing signal. Prior to the signal, sell and buy orders are held by the auctioneer and can be deleted or updated.
The current version supports limit orders but no market orders.
'''

    // URL to the plugin's documentation
    def documentation = "http://grails.org/plugin/powertac-auctioneer-pda"

    def doWithWebDescriptor = { xml ->
        // TODO Implement additions to web.xml (optional), this event occurs before 
    }

    def doWithSpring = {
        // TODO Implement runtime spring config (optional)
    }

    def doWithDynamicMethods = { ctx ->
        // TODO Implement registering dynamic methods to classes (optional)
    }

    def doWithApplicationContext = { applicationContext ->
        // TODO Implement post initialization spring config (optional)
    }

    def onChange = { event ->
        // TODO Implement code that is executed when any artefact that this plugin is
        // watching is modified and reloaded. The event contains: event.source,
        // event.application, event.manager, event.ctx, and event.plugin.
    }

    def onConfigChange = { event ->
        // TODO Implement code that is executed when the project configuration changes.
        // The event is the same as for 'onChange'.
    }
}
