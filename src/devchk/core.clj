(ns devchk.core
  (:require
   [clojure.data.csv :as csv]
   [clojure.tools.namespace.repl]
   [clojure.java.shell :as shell]
   [clojure.string :refer [split]]
   [clojure.core.async :as async :refer [<! >! go-loop]]
   [cheshire.core :as json]))

(defn json-parse-string
  [string]
  (json/parse-string string true))

(defn read-report
  "if a report exists, create an index of results so we don't have to process them again"
  []
  (let [report-file (java.io.File. "report.json")]
    (when (.exists report-file)
      (with-open [rdr (clojure.java.io/reader report-file)]
        (into {} (mapv (fn [result]
                         (let [result (:message (json-parse-string result))]
                           [(:uri (:source result)) (:md5 result)]))
                       (line-seq rdr)))))))

(def report-idx (read-report))

(def cache-root "")
(def cache-dir (-> cache-root (str "cache") java.io.File. .getAbsolutePath str))
(def image-cache-dir (-> cache-root (str "image-cache") java.io.File. .getAbsolutePath str))
(def clear-image-cache true) ;; set to false to preserve image cache (good for debugging)

;; number of parallel image processors to run, one per-core
(def num-processors (-> (Runtime/getRuntime) .availableProcessors))

;; hit the production iiif server for image requests. (good for debugging)
(def use-prod-iiif true)

;; create our cache dirs, if they don't already exist
(-> cache-dir java.io.File. .mkdirs)
(-> image-cache-dir java.io.File. .mkdirs)

(def image-chan
  "this is where we'll put the links to images we find while walking the API that need to be processed.
  accepts N images before blocking"
  (async/chan 10))

(def results-chan
  "this is where we'll put the results of processing the image.
  accepts N results before blocking"
  (async/chan 10))

(def stats
  "this is where we accumulate some ongoing stats about the state of the program as it's running."
  (atom {:num-images 0
         :num-processed 0
         :num-deviations 0
         :num-errors 0}))

;; utils

(defn increment
  "helper function that bumps stats"
  [what]
  (swap! stats update-in [what] inc))

(let [lock (Object.)]
  (defn stderr
    "synchronised printing to stderr. ensures everybody gets a line on stdout to themselves."
    [& args]
    (locking lock
      (binding [*out* *err*]
        (apply println args)
        (flush)))))

(let [lock (Object.)]
  (defn log
    "synchronised printing. ensures everybody gets a line on stdout to themselves"
    [level & args]
    (locking lock
      (let [filename (-> level name (str ".json"))
            extra (rest args)
            msg (json/generate-string (if (not (empty? extra))
                                                 {:message (first args) :extra extra}
                                                 {:message (first args)}))]
        (spit filename (str msg "\n") :append true)))))

(defn fs-safe-cache-key
  "safely encode a URI to something that can live cached on the filesystem"
  [url]
  (let [enc (java.util.Base64/getUrlEncoder)]
    (->> url
      str .getBytes (.encodeToString enc) str)))

(defn spy
  [x]
  (stderr "(spying)" x)
  x)

(defn sh
  "calls `shell/sh` with given `args`, printing a friendly version of the command to stdout before doing so."
  [& args]
  (log :debug (str "$ " (clojure.string/join " " args)))
  (apply shell/sh args))

(defn GET
  "fetch url, write to file, returns text"
  [url]
  (let [output-file (str cache-dir "/" (fs-safe-cache-key url))]
    (if (.exists (java.io.File. output-file))
      (slurp output-file)
      (let [{:keys [:exit :err :out]} (sh "curl" "-sS" url)]
        (log :debug (format "cache miss for '%s': %s" url output-file))
        (if (zero? exit)
          (do (spit output-file out) out)
          (log :error err))))))

(defn GET-binary
  "fetch url, write output to given output-file. returns output file on success"
  [url output-file]
  (if (and (.exists (java.io.File. output-file))
           (> (.length (java.io.File. output-file)) 0))
    output-file
    (let [{:keys [:exit :err :out]} (sh "curl" "--retry" "2" "-sS" url "--output" output-file)]
      (log :debug (format "cache miss for '%s': %s" url output-file))
      (if (zero? exit)
        output-file
        (log :error err)))))


(defn url?
  "predicate, returns true if given value can be parsed as a URL"
  [x]
  (try
    (java.net.URL. x)
    true
    (catch Exception e
      false)))

(defn msid?
  "predicate, returns true if given value can be parsed as a manuscript ID"
  [x]
  (try
    (-> x java.lang.Integer/valueOf pos-int?)
    (catch Exception e
      false)))

(defn delete-file
  [path]
  (when-let [obj (-> path java.io.File.)]
    (when (.isFile obj)
      (.delete obj))))

;; api handling


(defn -find-images
  "visits everything in `data` and looks for images.
  when an image is found it is added to the `image-chan`."
  [data]
  (let [pred (fn [val]
               (and (map? val)
                    (-> val :type (= "image"))))

        extractor (fn [val]
                    (let [image (:image val)
                          ;; {:label ... :title ... :id ... :size {...} :source ...}
                          ;; for example:
                          ;; {:label "Figure 11",
                          ;;  :title "Strategy to fit Gompertz function parameters on a simulated growth curve with noise added.",
                          ;;  :id "fig11",
                          ;;  :size {:width 1412, :height 624},
                          ;;  :source {:mediaType "image/jpeg",
                          ;;           :uri "https://iiif.elifesciences.org/lax:56613%2Felife-56613-fig11-v2.tif/full/full/0/default.jpg",
                          ;;           :filename "elife-56613-fig11-v2.jpg"}}
                          struct (merge (select-keys val [:label :title :id])
                                        (select-keys image [:size :source]))]
                      (async/>!! image-chan struct)
                      (increment :num-images)))]
    (if (pred data)
      ;; we've found some images! no need to recurse any deeper
      (extractor data)
      ;; we didn't find any images at this level, descend deeper
      (cond
        ;; if we're a map, visit each key+val and test each val
        (map? data) (run! (fn [[key val]]
                            [key (-find-images val)]) data)
        ;; if we're a list of data, test each one
        (sequential? data) (run! -find-images data)

        ;; scalar value probably, just exit
        :else nil))))

(defn find-images
  [data]
  (-find-images data))

(defn get-article
  [msid]
  (let [api "https://api.elifesciences.org"
        article-url (str api "/articles/" msid)
        keys-as-keywords true]
    (-> article-url GET (json/parse-string keys-as-keywords))))

(defn articles
  []
  (let [url "https://observer.elifesciences.org/report/published-article-index.csv"
        csv-data (doall (csv/read-csv (GET url)))

        ;; these are lazy fetches and won't actually happen until we hit `find-images` below
        article-id-list (->> csv-data rest (map first))
        article-list (map get-article (take 15 article-id-list))]

    ;; images are embedded throughout the article-json. we need to visit every value and look for these:
    ;; {"assets" [
    ;;     {"caption" [{"text" "...", "type" "paragraph"}],
    ;;      "id" "fig2",
    ;;      "image" {"alt" "",
    ;;               "uri" "https://iiif.elifesciences.org/lax:56613%2Felife-56613-fig2-v2.tif",
    ;;               "size" {"width" 3605,
    ;;                       "height" 2958},
    ;;               "source" {"mediaType" "image/jpeg",
    ;;                         "uri" "https://iiif.elifesciences.org/lax:56613%2Felife-56613-fig2-v2.tif/full/full/0/default.jpg",
    ;;                         "filename" "elife-56613-fig2-v2.jpg"}},
    ;;      "label" "Figure 2",
    ;;      "title" "Growth of Mtb strain H37Rv during exposure to ofloxacin.",
    ;;      "type" "image"}
    ;; ]}

    (run! find-images article-list))
  nil)

(defn find-all
  "finds sources of iiif data and extracts them. Results are put on the `image-chan`."
  []
  (async/go
    (articles)
    ;; digests?
    ;; no more data to scrape, signal the chan to close
    (async/close! image-chan)
    ))

(defn find-single-image
  [iiif-url]
  (async/put! image-chan {:label "unknown label"
                          :title "unknown title"
                          :id "unknown"
                          :size {:width 0 :height 0}
                          :source {:uri iiif-url}})
  (increment :num-images))

(defn find-single-article
  [msid]
  (log :debug "got msid" msid)
  (find-images (get-article msid)))


;; processing


(defn compare-images
  "given two images, calls ImageMagick with the right incantation.
  Call to ImageMagick is wrapped in a 'timeout' of a 20 seconds with a --kill after 40 seconds.
  If you're running this script without a swap disk, and somehow manage to max out your ram and
  the kernel oom killer hasn't run yet, 'timeout' will catch you."
  [image-path-1 image-path-2 & [attempt]]
  (let [output-path (str (java.io.File/createTempFile "devchk" ".jpg"))

        {:keys [exit err out]}
        (sh "timeout" "--kill-after=40s" "20s"
            ;;"magick" "compare" "-metric" "pae" ;; IM 7 only. ubuntu 18.04 still on IM 6
            "compare" "-metric" "pae"
            "-quiet"
            image-path-1 image-path-2 output-path)

        ;; 'ae' is Absolute Error - the number of pixels that are different between the two
        ;; we're not really interested in this value
        ;; 'pae' is Peak Absolute Error, a value between 0 and 1 of the maximum difference between two pixels
        ;; 0 is perfectly identical
        ;; 1 is completely different
        ;; setting the fuzz factor to this value as a percentage will get you a successful result
        [ae pae] (rest (re-matches #"(\d+) \((\d\.\d+)\)" err))

        pae (some-> pae java.lang.Float/valueOf)]
    
    (cond
      ;; first failure, try again
      (and (> exit 1)
           (= attempt nil)) (compare-images image-path-1 image-path-2 2)

      ;; image magick died. probably because iiif died and returned text.
      ;; this isn't the first attempt either. log an error
      (> exit 1) (do
                   (increment :num-errors)
                   (log :error "imagemagick return an exit code greater than 1"
                        {:stderr err
                         :stdout out
                         :exit exit
                         :image-1 image-path-1
                         :image-2 image-path-2})))

      {:pae pae
       :cache {:local-comparison-file output-path
               :local-original-file image-path-1
               :local-iiif-file image-path-2}}))

(defn image-cache-path
  "images must be stored on the disk long enough to compare them with the iiif derived image.
  there is no guaranteee there won't be a naming conflict so preserve the path structure."
  [image-url]
  (let [;; /digests/55692/digest-55692.tif
        path (-> image-url java.net.URL. .getPath java.net.URLDecoder/decode)
        ;; digest-55692.tif
        file (-> path java.io.File. .getName)
        ;; /digests/55692/
        parent (-> path java.io.File. .getParent)
        ;; ./image-cache/digests/55692/
        cache-dir-obj (java.io.File. (str image-cache-dir parent))]
    ;; create the directory if it doesn't exist
    (when-not (.exists cache-dir-obj)
      (-> cache-dir-obj .mkdirs))
    (str cache-dir-obj "/" file)))

(defn download-image
  "downloads a binary image, returns path to file within the `image-cache`"
  [image-url]
  (let [output-file (image-cache-path image-url)]
    (GET-binary image-url output-file)))

(defn iiif-to-s3-url
  "converts a iiif URL to an S3 bucket URL where the original image is stored."
  [iiif-url]
  ;; transformation looks like:
  ;; https://iiif.elifesciences.org/digests/55692%2Fdigest-55692.tif/full/full/0/default.webp
  ;; /digests/55692%2Fdigest-55692.tif/full/full/0/default.webp
  ;; /digests/55692/digest-55692.tif/full/full/0/default.webp
  ;; /digests/55692/digest-55692.tif
  ;; https://prod-elife-published.s3.amazonaws.com/digests/55692/digest-55692.tif
  (let [strip-from-full (fn [path]
                          (last (re-find #"(.+)?/full/full/.*" path)))
        lax-to-articles (fn [path]
                          (if (clojure.string/starts-with? path "/lax:")
                            (str "/articles/" (subs path 5))
                            path))
        prepend-s3 (fn [path]
                     (str "https://prod-elife-published.s3.amazonaws.com" path))]
    (-> iiif-url
        java.net.URL.
        .getPath
        java.net.URLDecoder/decode
        strip-from-full
        lax-to-articles
        prepend-s3)))

(defn image-meta
  "returns additional image data"
  [image-path]
  (let [img-obj (java.io.File. image-path)]
    {:bytes (.length img-obj)
     :md5 (first (split (:out (sh "md5sum" "--binary" image-path)) #" "))}))

(defn -process-image
  "given an image, generates urls, downloads the images, compares them, tacks on extra image data and returns a map of results"
  [image]
  (let [iiif-url (-> image :source :uri)]
    (if (contains? report-idx iiif-url)
      (log :debug "skipping" iiif-url) ;; already have a result for this one

      (let [original-image-url (iiif-to-s3-url iiif-url)
            original-image (download-image original-image-url)
            local-iiif-url (str "http://localhost" (-> iiif-url java.net.URL. .getPath))
            iiif-image (download-image (if use-prod-iiif iiif-url local-iiif-url))]

        (cond
          (not iiif-image) (log :error "problem downloading iiif image:" {:iiif-image iiif-image})
          (not original-image) (log :error "problem downloading original image:" {:original-image original-image})

          :else
          (let [;;iiif-meta (image-meta iiif-image)
                ;;iiif-meta (assoc iiif-meta :local-uri local-iiif-url)
                iiif-meta (-> iiif-image image-meta (assoc :local-uri local-iiif-url))

                original-meta (image-meta original-image)

                article-id (->> iiif-url (re-find #"elife\-(\d{5})\-") last java.lang.Integer/valueOf)
                comparison-results (compare-images original-image iiif-image)

                result (merge image original-meta)
                result (merge result {:uri original-image-url :article-id article-id})
                result (update-in result [:source] merge iiif-meta)
                result (merge result comparison-results)]
            result))))))

(defn process-image
  "guard function"
  [image]
  (try
    (-process-image image)
    (catch Exception uncaught-exc
      (log :error "uncaught exception processing image" {:image image})
      (increment :num-errors))))

(defn image-processor
  "this will take images off of `image-chan` and 'processes' them - runs each image through a series of tests.
  the results are put on the `results-chan`."
  [my-id]
  (stderr "started image processor" my-id)
  (async/go-loop []
    (if-let [image (async/<! image-chan)]
      (let [result (process-image image)]
        (when result
          (log :debug (str "processor:" my-id " - processing image - " (:label image)))
          (async/>! results-chan result))
        (increment :num-processed)
        (recur))

      ;; received nil, no more results on this channel, exit
      (stderr (format "worker %s exiting" my-id))))
  nil)


;; reporting


(defn report
  "this takes results off of `results-chan` and write a report to stdout periodically.
  we do this on the main thread so it blocks us from exiting until we have no more results left."
  []
  (loop []
    (when-let [result (async/<!! results-chan)]

      ;; check for deviations
      (if (or (= (:pae result) 1)
              (= (:pae result) 1.0))
        (increment :num-deviations)

        ;; no errors to investigate, delete the cached files
        (when clear-image-cache
          (some-> result :cache :local-comparison-file delete-file)
          (some-> result :cache :local-iiif-file delete-file)
          (some-> result :cache :local-original-file delete-file)))

      ;; ...

      (let [stats @stats
            num-remaining (- (:num-images stats)
                             (:num-processed stats))]

        ;; (controls the output somewhat but isn't guaranteed)
        (stderr (select-keys stats [:num-images :num-processed :num-errors :num-deviations]))
        (log :report result)

        (when-not (= num-remaining 0)
          ;; jobs outstanding, pull next job channel
          (recur))))))

;;

(defn parse-args
  [arg-list]
  (when-let [arg (first arg-list)]
    (cond
      (url? arg) [arg nil]
      (msid? arg) [nil arg]
      :else (throw (RuntimeException. "given argument is neither a url nor a manuscript id")))))

(defn -main
  [& args]
  (let [[iiif-url msid] (parse-args args)]

    (cond
      ;; add image to image-chan
      (some? iiif-url) (find-single-image iiif-url)

      ;; fetch article, extract image data
      (some? msid) (find-single-article msid)

      ;; no args given, find all data
      :else (find-all))

    ;; start processing images added by find-data
    (run! image-processor (range num-processors))

    ;; polls the results channel and prints progress
    ;; will exit if no new results after `report-timeout` seconds
    (report)

    ;; write the final set of stats to the report
    (log :report @stats)

    ;; clean up
    (async/close! results-chan)))
