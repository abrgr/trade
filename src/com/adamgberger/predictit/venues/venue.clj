(ns com.adamgberger.predictit.venues.venue
  (:gen-class))

(defprotocol Venue
  (id [venue])
  (current-available-balance [venue])
  (available-markets [venue])
  (positions [venue])
  (monitor-order-book [venue market-id market-name contract-id on-update continue-monitoring]))
