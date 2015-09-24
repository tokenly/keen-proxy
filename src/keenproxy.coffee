###
#
# Listens for events from beanstalk and forwards them to keen.io
#
###

beanstalkHost   = process.env.BEANSTALK_HOST  or '127.0.0.1'
beanstalkPort   = process.env.BEANSTALK_PORT  or 11300
DEBUG           = !!(process.env.DEBUG        or false)
EVENTS_TUBE     = process.env.EVENTS_TUBE     or 'keen_events'
MAX_RETRIES     = process.env.MAX_RETRIES     or 30
MAX_QUEUE_SIZE  = process.env.MAX_QUEUE_SIZE  or 5
CLIENT_TIMEOUT  = process.env.CLIENT_TIMEOUT  or 10000  # <-- clients must respond in this amount of time

KEEN_PROJECT_ID = process.env.KEEN_PROJECT_ID  or false
KEEN_API_KEY    = process.env.KEEN_API_KEY     or false

SLACK_WEB_API_TOKEN = process.env.SLACK_WEB_API_TOKEN or false
SLACK_USERNAME      = process.env.SLACK_USERNAME      or 'Swapbot'
SLACK_CHANNEL       = process.env.SLACK_CHANNEL       or ''
SLACK_ICON_URL      = process.env.SLACK_ICON_URL      or 'https://tokenrank.tokenly.com/images/blank-robot-fade.png'

if not KEEN_PROJECT_ID
    console.error "You must supply a KEEN_PROJECT_ID environment variable"
    process.exit 1

if not KEEN_API_KEY
    console.error "You must supply a KEEN_API_KEY environment variable"
    process.exit 1



http        = require('http')
nodestalker = require('nodestalker')
figlet      = require('figlet')
rest        = require('restler')
moment      = require('moment')

# get one global write client
RETRY_PRIORITY = 11
RETRY_DELAY    = 5 # <-- backoff is this times the number of attempts
                   #     total time is 1*5 + 2*5 + 3*5 + ...

MAX_SHUTDOWN_DELAY = CLIENT_TIMEOUT + 1000  # <-- when shutting down, never wait longer than this for a response from any client

# listen
jobCount = 0
reserveJob = ()->
    if jobCount >= MAX_QUEUE_SIZE
        if DEBUG then console.log "[#{new Date().toString()}] jobCount of #{jobCount} has reached maximum.  Delaying."
        setTimeout(reserveJob, 500)
        return

    # if DEBUG then console.log "[#{new Date().toString()}] connecting to beanstalk"
    beanstalkReadClient = nodestalker.Client("#{beanstalkHost}:#{beanstalkPort}")
    beanstalkReadClient.watch(EVENTS_TUBE).onSuccess ()->
        beanstalkReadClient.reserve().onSuccess (job)->
            ++jobCount

            # watch for another job
            reserveJob()

            jobData = JSON.parse(job.data)

            if jobData.meta.jobType == 'slack'
                jobHandler = processSlackJob

            else
                jobHandler = processKeenStatsJob
            

            jobHandler jobData, job, (result)->
                --jobCount
                if DEBUG then console.log "[#{new Date().toString()}] deleting job #{job.id}"
                beanstalkReadClient.deleteJob(job.id).onSuccess (del_msg)->
                    # deleted
                    #   end this connection
                    beanstalkReadClient.disconnect()
                    return

                # job processed
                return
            return
        return
    return


processKeenStatsJob = (jobData, job, callback)->
    if not KEEN_API_KEY
        finishJob(true, 'No KEEN_API_KEY defined', jobData, job, callback)
        return

    href = "https://api.keen.io/3.0/projects/#{KEEN_PROJECT_ID}/events/#{jobData.meta.collection}?api_key=#{KEEN_API_KEY}"
    requestData = JSON.stringify(jobData.data)
    processRestlerJob(jobData, job, href, true, requestData, callback)

processSlackJob = (jobData, job, callback)->
    if not SLACK_WEB_API_TOKEN
        finishJob(true, 'No SLACK_WEB_API_TOKEN defined', jobData, job, callback)
        return

    attachment = jobData.data
    if not attachment.color then attachment.color = "good"

    href = "https://slack.com/api/chat.postMessage"
    slackRequestData = {
        token: SLACK_WEB_API_TOKEN
        username: SLACK_USERNAME
        channel: SLACK_CHANNEL
        icon_url: SLACK_ICON_URL
        text: ''
    }
    slackRequestData.attachments = JSON.stringify([attachment])

    # console.log "slackRequestData=",slackRequestData

    requestData = slackRequestData
    processRestlerJob(jobData, job, href, false, requestData, callback)

processRestlerJob = (jobData, job, href, isPost, requestData, callback)->
    success = false

    # call the callback
    if not jobData.meta?.attempt?
        jobData.meta.attempt = 0
    jobData.meta.attempt = jobData.meta.attempt + 1

    # console.log("requestData: ",requestData)

    try
        if DEBUG then console.log "[#{new Date().toString()}] begin processKeenStatsJob "+job.id+" (attempt #{jobData.meta.attempt} of #{MAX_RETRIES}, href #{href})"

        callParams = {
            headers: {'User-Agent': 'Tokenly Event Proxy'}
            timeout: CLIENT_TIMEOUT
        }

        if isPost
            callParams.headers['Content-Type'] = 'application/json'
            callFn = rest.post
            callParams.data = requestData
        else
            callFn = rest.get
            callParams.query = requestData

        callFn(href, callParams).on 'complete', (data, response)->
            console.log "data",data
            # console.log "response",response
            msg = ''
            if response
                if DEBUG then console.log "[#{new Date().toString()}] received HTTP response: "+response?.statusCode?.toString()
            else
                if DEBUG then console.log "[#{new Date().toString()}] received no HTTP response"
            if response? and response.statusCode.toString().charAt(0) == '2'
                success = true
            else
                success = false
                if response?
                    msg = "ERROR: received HTTP response with code "+response.statusCode
                else
                    if data instanceof Error
                        msg = ""+data
                    else
                        msg = "ERROR: no HTTP response received"

            # if DEBUG then console.log "[#{new Date().toString()}] #{job.id} finish success=#{success}"
            finishJob(success, msg, jobData, job, callback)
            return

        .on 'timeout', (e)->
            if DEBUG then console.log "[#{new Date().toString()}] #{job.id} timeout", e

            # 'complete' will not be called on a timeout failure
            finishJob(false, "Timeout: "+e, jobData, job, callback)
            
            return

        .on 'error', (e)->
            # 'complete' will be called after this
            if DEBUG then console.log "[#{new Date().toString()}] #{job.id} http error", e
            return

    catch err
         if DEBUG
            console.log "[#{new Date().toString()}] Caught ERROR:",err
            console.log(err.stack);

         finishJob(false, "Unexpected error: "+err, jobData, job, callback)
         return

    return

finishJob = (success, err, jobData, job, callback)->
    if DEBUG then console.log "[#{new Date().toString()}] end "+job.id+""

    # if done
    #   then push the job back to the beanstalk notification_result queue with the new state
    finished = false
    if success
        finished = true
    else
        # error
        if DEBUG then console.log "[#{new Date().toString()}] error - retrying | #{err}"
        if jobData.meta.attempt >= MAX_RETRIES
            if DEBUG then console.log "[#{new Date().toString()}] giving up after attempt #{jobData.meta.attempt}"
            finished = true


    if finished
        if DEBUG then console.log "[#{new Date().toString()}] finished | success=#{success}#{if err then ' | '+err else ''}"
        callback(true)
    else
        # retry
        insertJobIntoBeanstalk EVENTS_TUBE, jobData, RETRY_PRIORITY, RETRY_DELAY * jobData.meta.attempt, (loadSuccess)->
            if loadSuccess
                callback(true)
            return
    return



# beanstalk
insertJobIntoBeanstalk = (queue, data, retry_priority, retry_delay, callback)->
    beanstalkWriteClient = nodestalker.Client("#{beanstalkHost}:#{beanstalkPort}")
    beanstalkWriteClient.use(queue).onSuccess ()->
        beanstalkWriteClient.put(JSON.stringify(data), retry_priority, retry_delay)
        .onSuccess ()->
            # if DEBUG then console.log "job loaded"
            callback(true)
            return
        .onError ()->
            if DEBUG then console.log "[#{new Date().toString()}] error loading job to #{queue}"
            callback(false)
        return
    .onError ()->
        if DEBUG then console.log "[#{new Date().toString()}] error connecting to beanstalk"
        callback(false)
    return




gracefulShutdown = (callback)->
    startTimestamp = new Date().getTime()
    if DEBUG then console.log "[#{new Date().toString()}] begin shutdown"

    intervalReference = setInterval(()->
        if jobCount == 0 or (new Date().getTime() - startTimestamp >= MAX_SHUTDOWN_DELAY)
            if jobCount > 0
                if DEBUG then console.log "[#{new Date().toString()}] Gave up waiting on #{jobCount} job(s)"
            if DEBUG then console.log "[#{new Date().toString()}] shutdown complete"
            clearInterval(intervalReference)
            callback()
        else
            if DEBUG then console.log "[#{new Date().toString()}] waiting on #{jobCount} job(s)"
    , 250)

# signal handler
process.on "SIGTERM", ->
    if DEBUG then console.log "[#{new Date().toString()}] caught SIGTERM"
    gracefulShutdown ()->
        process.exit 0
        return

process.on "SIGINT", ->
    if DEBUG then console.log "[#{new Date().toString()}] caught SIGINT"
    gracefulShutdown ()->
        process.exit 0
        return
    return


figlet.text('Tokenly Keen Proxy', 'Slant', (err, data)->
    process.stdout.write data
    process.stdout.write "\n\n"
    process.stdout.write "connecting to beanstalkd at "+beanstalkHost+":"+beanstalkPort+"\n\n"

    return
)

# run the reserver
setTimeout ()->
    reserveJob()
, 10


