(ns com.adamgberger.predictit.venues.venue
  (:gen-class))

(defprotocol Venue
  (id [venue])
  (current-available-balance [venue send-result])
  (available-markets [venue send-result])
  (positions [venue send-result])
  (contracts [venue market-id full-market-url send-results])
  (orders [venue market-id full-market-url contract-id send-results])
  (order-book [venue market-id full-market-url contract-id send-results])
  (submit-order [venue mkt-id contract-id trade-type qty price send-results])
  (cancel-order [venue mkt-id order-id send-results]))
