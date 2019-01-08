(ns com.adamgberger.predictit.venues.venue
  (:gen-class))

(defprotocol Venue
  (id [venue])
  (current-available-balance [venue])
  (available-markets [venue])
  (positions [venue])
  (contracts [venue market-id full-market-url])
  (monitor-order-book [venue market-id full-market-url contract-id]))
