(ns io.singer
  (:require [clojure.data.json :as json]
            [clojure.tools.cli :as cli])
  (:import [java.io BufferedReader]))

(defn require [m k]
  (or (m k)
      (throw (Exception. (str k " is missing in " m)))))

(defn require-string [m k]
  (let [v (require m k)]
    (if (string? v)
      v
      (throw (Exception. (str k  " must be a string, got " v " in " k))))))

(defn- msg-type [m]
  (require-string m "type"))

(defn- msg-stream [m]
  (require-string m "stream"))

(defn- msg-schema [m]
  (require m "schema"))

(defn- msg-key-properties [m]
  (let [res (m "key_properties")]
    (if (and (or  (list? res)
                  (vector? res))
             (every? string? res))
      res
      (throw (Exception. (str "key_properties must be a list of strings, got " res " in " m))))))

(defn- msg-record [m] (require m "record"))
(defn- msg-value [m] (require m "value"))

(defn parse [s]

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

(defn reader []
  (BufferedReader. *in*))

(defn next-message [rdr]
  (let [s (.readLine rdr)]
    (parse s)))

(defn write-message [m]
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

(comment

(parse-message "{\"type\": \"RECORD\", \"record\": {\"name\": \"mike\"}, \"stream\": \"people\"}")

(parse-message "{\"type\": \"SCHEMA\", \"schema\": {\"type\": \"object\", \"properties\": {\"id\": \"integer\", \"name\": \"string\"}}, \"stream\": \"people\", \"key_properties\": [\"id\"]}")

(parse-message "{\"type\": \"STATE\", \"value\": {\"seq\": 1}}")


(write-record "people" {"id" 1 "name" "mike"})
(write-state {"seq" 1})
(write-schema "people"
              {"type" "object" "properties" {"id" "integer" "name" "string"}}
              ["id"]))


