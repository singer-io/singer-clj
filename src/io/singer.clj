(ns io.singer
  (:require [clojure.data.json :as json]
            [clojure.tools.cli :as cli]
            [clojure.java.io :as io])
  (:import [java.io BufferedReader]))

;;; Private helper functions for parsing messages

(defn- require-key [m k]
  (or (m k)
      (throw (Exception. (str k " is missing in " m)))))

(defn- require-string [m k]
  (let [v (require-key m k)]
    (if (string? v)
      v
      (throw (Exception. (str k  " must be a string, got " v " in " k))))))

(defn- msg-type [m]
  (require-string m "type"))

(defn- msg-stream [m]
  (require-string m "stream"))

(defn- msg-schema [m]
  (require-key m "schema"))

(defn- msg-key-properties [m]
  (let [res (m "key_properties")]
    (if (and (or  (list? res)
                  (vector? res))
             (every? string? res))
      res
      (throw (Exception. (str "key_properties must be a list of strings, got " res " in " m))))))

(defn- msg-record [m] (require-key m "record"))
(defn- msg-value [m] (require-key m "value"))

;;; Public functions

(defn parse [s]
  "Parses a message and returns it as a map"
  (let [m (json/read-str s)]
    (when-not (map? m)
      (throw (Exception. "Message must be a map, got" s)))
      (case (msg-type m)

        "RECORD"
        {::type ::record
         ::stream (msg-stream m)
         ::record (m "record")}

        "SCHEMA"
        {::type ::schema
         ::stream (msg-stream m)
         ::key-properties (msg-key-properties m)
         ::schema (msg-schema m)}

        "STATE"
        {::type ::state
         ::value (m "value")})))

(defn next-message
  "Reads the next message from *in* and parses it."
  []
  (when-let [s (.readLine *in*)]
    (parse s)))

(defn write-message
  "Writes a message to *out*"
  [m]
  (json/write
   (case (::type m)
    ::record
    {"type" "RECORD"
     "stream" (::stream m)
     "record" (::record m)}

    ::schema
    {"type" "RECORD"
     "stream" (::stream m)
     "schema" (::schema m)
     "key_properties" (::key-properties m)}

    ::state
    {"type" "STATE"
     "value" (::value m)})
   *out*))

(defn write-record [stream record]
  (write-message {::type ::record ::stream stream ::record record}))

(defn write-state [value]
  (write-message {::type ::state, ::value value}))

(defn write-schema [stream schema key-properties]
  (write-message {::type ::schema ::stream stream ::schema schema ::key-properties key-properties}))

(defn load-json [path]
  (with-open [r (io/reader path)]
    (json/read r)))

(def opt-config
  ["-c" "--config CONFIG" "Config JSON file"
   :parse-fn load-json])

(def opt-state
  ["-s" "--state STATE" "State JSON file"
   :parse-fn load-json])

(def tap-options
  [opt-config
   opt-state])

(def target-options
  [opt-config])

(defn parse-tap-opts [args]
  (cli/parse-opts args tap-options))
