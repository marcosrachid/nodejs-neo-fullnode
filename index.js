/**
 * Sync Mainnet
 * The most stable approach to sync blocks is to ignore missing blocks and redundancy checks.
 * You may resolve those concerns are separate script, while this one will purely focus on
 * fetching new blocks incrementally as fast as it can.
 */
const Neo = require('@cityofzion/neo-js').Neo
const _ = require('lodash')
const moment = require('moment')

process.on('unhandledRejection', (reason, promise) => {
  console.warn('Unhandled rejection. Reason:', reason)
})

// -- Parameters
const storageType = 'mongodb'
const dbConnectionString = 'mongodb://localhost/neo_mainnet'
const blockCollectionName = 'blocks'

// -- Implementation

;(async () => {
  console.log('== Sync Mainnet ==')

  /**
   * Neo instantiation along with customizations.
   * Notice that we did not specific network option as we will be providing endpoints explicitly instead.
   * Be sure that the database is running ready for sync.
   */
  const neo = new Neo({
    network: 'mainnet',
    storageType,
    storageOptions: {
      connectionString: dbConnectionString,
      collectionNames: {
        blocks: blockCollectionName,
      },
      loggerOptions: { level: 'info' },
    },
    meshOptions: {
      startBenchmarkOnInit: true,
      toFetchUserAgent: true,
      benchmarkIntervalMs: 2000,
      fetchMissingUserAgentIntervalMs: 5000,
      refreshUserAgentIntervalMs: 5 * 60 * 1000,
      minActiveNodesRequired: 2,
      pendingRequestsThreshold: 5,
      loggerOptions: { level: 'warn' },
    },
    apiOptions: {
      loggerOptions: { level: 'info' },
    },
    syncerOptions: {
      minHeight: 1,
      maxHeight: undefined,
      blockRedundancy: 1,
      checkRedundancyBeforeStoreBlock: true,
      startOnInit: true,
      toSyncIncremental: true,
      toSyncForMissingBlocks: true,
      toPruneRedundantBlocks: true,
      storeQueueConcurrency: 30,
      pruneQueueConcurrency: 10,
      enqueueBlockIntervalMs: 5000,
      verifyBlocksIntervalMs: 1 * 60 * 1000,
      maxStoreQueueLength: 1000,
      retryEnqueueDelayMs: 5000,
      standardEnqueueBlockPriority: 5,
      retryEnqueueBlockPriority: 3,
      missingEnqueueStoreBlockPriority: 1,
      enqueuePruneBlockPriority: 5,
      maxPruneChunkSize: 1000,
      loggerOptions: { level: 'info' }
    },
    loggerOptions: { level: 'info' },
  })

  /**
   * Print blockchain's height upon neo.mesh is ready.
   */
  neo.mesh.on('ready', async () => {
    console.log('neo.mesh is ready.')
    try {
      const blockchainHeight = await neo.mesh.getHighestNode().blockHeight
      console.log('Blockchain Height:', blockchainHeight)
    } catch (err) {
      console.warn('neo.mesh.getHighestNode().blockHeight failed. Message:', err.message)
    }
  })

  /**
   * Print storage's height upon neo.storage is ready.
   */
  neo.storage.on('ready', async () => {
    console.log('neo.storage is ready.')
    try {
      const storageBlockCount = await neo.storage.getBlockCount()
      console.log('Highest Count in Storage:', storageBlockCount)
    } catch (err) {
      console.warn('neo.storage.getBlockCount() failed. Message:', err.message)
    }
  })

  /**
   * Print a message when the storage is detected to be 'up to date'.
   */
  neo.syncer.on('UpToDate', () => {
    console.log('Storage sync is now up-to-date!')
  })

  /**
   * While syncing is happening, we will be building a performance report to keep
   * track of syncing status.
   */
  const report = {
    successCount: 0,
    failedCount: 0,
    missingCount: undefined,
    excessiveCount: undefined,
    blockchainHeight: undefined,
    syncHeight: undefined,
    startAt: moment(),
  }
  neo.syncer.on('storeBlock:complete', (payload) => {
    if (payload.isSuccess) {
      report.successCount += 1
      if (!report.syncHeight || payload.height > report.syncHeight) {
        report.syncHeight = payload.height
      }
    } else {
      report.failedCount += 1
    }
  })
  neo.syncer.on('blockVerification:missingBlocks', (payload) => {
    console.log('! Updated missing block count:', payload.count)
    report.missingCount = payload.count
  })
  neo.syncer.on('blockVerification:excessiveBlocks', (payload) => {
    console.log('! Updated excessive block count:', payload.count)
    report.excessiveCount = payload.count
  })

  // Generate sync report
  const SYNC_REPORT_INTERVAL_MS = 5 * 1000
  setInterval(() => {
    if (report.successCount === 0) {
      console.log('No sync progress yet')
      return
    }

    const node = neo.mesh.getHighestNode()
    if (!node) {
      console.warn('Problem with neo.mesh.getHighestNode().')
      return
    }

    report.blockchainHeight = node.blockHeight
    const msElapsed = moment().diff(report.startAt)
    const trueSyncCount = (report.missingCount) ? report.syncHeight - report.missingCount : report.syncHeight
    const completionPercentage = _.round((trueSyncCount / report.blockchainHeight * 100), 4) + '%'
    const blockCountPerMinute = _.round((report.successCount / msElapsed * 1000 * 60), 0)
    const failPercentage = _.round((report.failedCount / (report.successCount + report.failedCount) * 100), 4) + '%'

    let missingPercentage = 'N/A'
    if (report.missingCount !== undefined) {
      missingPercentage = _.round((report.missingCount / report.syncHeight * 100), 4) + '%'
    }

    let excessivePercentage = 'N/A'
    if (report.excessiveCount !== undefined) {
      excessivePercentage = _.round((report.excessiveCount / trueSyncCount * 100), 4) + '%'
    }

    const requiredMs = _.round((report.blockchainHeight - trueSyncCount) / (report.successCount / msElapsed), 0)
    const etaText = moment.duration(requiredMs).humanize(true)

    console.log(`[${moment().utc().format('HH:mm:ss')}] ${trueSyncCount}/${report.blockchainHeight} | complete: ${completionPercentage} | ${blockCountPerMinute} blocks/min (ETA ${etaText}) | failure: ${failPercentage} | missing: ${missingPercentage} | excessive: ${excessivePercentage}`)
  }, SYNC_REPORT_INTERVAL_MS)

  // Generate nodes report
  const NODES_REPORT_INTERVAL_MS = 60 * 1000
  setInterval(() => {
    console.log('## NODES REPORT:')
    const nodePool = _.filter(neo.mesh.nodes, (n) => (n.isActive && n.pendingRequests > 0))
    if (nodePool.length === 0) {
      console.log('> No active pending nodes')
    }

    nodePool.forEach((n) => {
      console.log(`> pending: ${n.pendingRequests} | latency: ${n.latency}| height: ${n.blockHeight} | UA: ${n.userAgent} | ${n.endpoint}`)
    })
  }, NODES_REPORT_INTERVAL_MS)

  // Generate syncer report
  const SYNCER_REPORT_INTERVAL_MS = 30 * 1000
  setInterval(() => {
    console.log('## SYNCER REPORT:')
    console.log('> blockWritePointer:', neo.syncer.blockWritePointer)
    console.log('> storeQueue length:', neo.syncer.storeQueue.length())
  }, SYNCER_REPORT_INTERVAL_MS)
})()
