package xsbt.boot

import java.util.Arrays
import java.util.Properties
import java.io.FileOutputStream
import java.util.concurrent.Executors

import org.apache.ivy.core.IvyContext
import org.apache.ivy.core.event.EventManager
import org.apache.ivy.core.event.download.PrepareDownloadEvent
import org.apache.ivy.core.event.resolve.StartResolveEvent
import org.apache.ivy.core.event.resolve.EndResolveEvent
import org.apache.ivy.core.module.descriptor.Artifact
import org.apache.ivy.core.module.descriptor.ModuleDescriptor
import org.apache.ivy.core.module.descriptor.DependencyDescriptor
import org.apache.ivy.core.module.id.ModuleRevisionId
import org.apache.ivy.core.report._
import org.apache.ivy.core.resolve._
import org.apache.ivy.core.sort.SortEngine
import org.apache.ivy.util.Message
import org.apache.ivy.util.filter.Filter
import org.apache.ivy.plugins.resolver.DependencyResolver

import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, ExecutionContext, Future }

private[xsbt] case class DownloadResult(
  dep: IvyNode,
  report: DownloadReport,
  totalSizeDownloaded: Long
)

object ParallelResolveEngine {
  private val resolveExecutionContext = ParallelExecution.executionContext
}

/** Define an ivy [[ResolveEngine]] that resolves dependencies in parallel. */
private[xsbt] class ParallelResolveEngine(
  settings: ResolveEngineSettings,
  eventManager: EventManager,
  sortEngine: SortEngine
)
  extends ResolveEngine(settings, eventManager, sortEngine) {

  /* the resolve operation is a huge bottleneck it looks */
  override def resolve(md: ModuleDescriptor, options: ResolveOptions) = {
    val oldDictator: DependencyResolver = getDictatorResolver()
    val context: IvyContext = IvyContext.getContext()
    try {
      val confs = options.getConfs(md)
      options.setConfs(confs)

      if (options.getResolveId() == null) {
        options.setResolveId(ResolveOptions.getDefaultResolveId(md))
      }

      eventManager.fireIvyEvent(new StartResolveEvent(md, confs))

      val start = System.currentTimeMillis()
      val transitive =
        if (options.isTransitive()) ""
        else " [not transitive]"
      // if (ResolveOptions.LOG_DEFAULT.equals(options.getLog())) {
      //   Message.info(":: resolving dependencies :: " + md.getResolvedModuleRevisionId() + transitive)
      //   Message.info("\tconfs: " + Arrays.asList(confs))
      // } else {
      Message.verbose(":: resolving dependencies :: " + md.getResolvedModuleRevisionId() + transitive)
      Message.verbose("\tconfs: " + Arrays.asList(confs))
      // }
      Message.verbose("\tvalidate = " + options.isValidate())
      Message.verbose("\trefresh = " + options.isRefresh())

      println("RESOLVE IMPL 1")
      val report = new ResolveReport(md, options.getResolveId())

      println("RESOLVE IMPL 2")
      val data = new ResolveData(this, options)
      context.setResolveData(data)

      println("RESOLVE IMPL 3")
      val dependencies = getDependencies(md, options, report)
      import scala.collection.JavaConverters._
      println("RESOLVE IMPL 3 1/2")
      report.setDependencies(dependencies.toList.asJava, options.getArtifactFilter())

      println("RESOLVE IMPL 4")
      if (options.getCheckIfChanged()) {
        println("RESOLVE IMPL 5")
        report.checkIfChanged()
      }

      println("RESOLVE IMPL 6")
      // produce resolved ivy file and ivy properties in cache
      val cacheManager = settings.getResolutionCacheManager()
      cacheManager.saveResolvedModuleDescriptor(md)

      println("RESOLVE IMPL 7")
      // we store the resolved dependencies revisions and statuses per asked dependency
      // revision id, for direct dependencies only.
      // this is used by the deliver task to resolve dynamic revisions to static ones
      val ivyPropertiesInCache = cacheManager.getResolvedIvyPropertiesInCache(md.getResolvedModuleRevisionId())

      val props = new Properties()
      if (dependencies.length > 0) {

        println("RESOLVE IMPL start of for loops")

        val root = dependencies(0).getRoot()

        implicit val ec = ParallelResolveEngine.resolveExecutionContext
        import scala.collection.JavaConverters._

        println("RESOLVE IMPL 8")
        val forcedRevisions = (for (dep <- dependencies) yield {
          if (dep.getModuleRevision() != null && dep.getModuleRevision().isForce())
            Some((dep.getModuleId() -> dep.getResolvedId()))
          else None
        }).flatten.toMap

        println("RESOLVE IMPL 9")
        val topLevelDeps = (for (dep <- dependencies) yield {
          if (!dep.hasProblem()) {
            val dd = dep.getDependencyDescriptor(root)
            if (dd != null) {
              Some((dep.getModuleId() -> dep))
            } else None
          } else None
        }).flatten.toMap

        val depsProcessing = dependencies.toSeq.map {
          case dep =>
            Future {
              if (!dep.hasProblem() && !dep.isCompletelyEvicted()) {
                val dd: DependencyDescriptor = {
                  val _dd = dep.getDependencyDescriptor(root)
                  if (_dd == null) {
                    topLevelDeps.get(dep.getModuleId()) match {
                      case Some(tlDep) => tlDep.getDependencyDescriptor(root)
                      case _           => _dd
                    }
                  } else _dd
                }

                if (dd != null) {
                  var depResolvedId = dep.getResolvedId()
                  var depDescriptor = dep.getDescriptor()
                  val depRevisionId = dd.getDependencyRevisionId()
                  val forcedRevisionId =
                    forcedRevisions.get(dep.getModuleId()).getOrElse(null)

                  if (dep.getModuleRevision() != null
                    && dep.getModuleRevision().isForce()
                    && !depResolvedId.equals(depRevisionId)
                    && !settings.getVersionMatcher().isDynamic(depRevisionId)) {
                    // if we were forced to this revision and we 
                    // are not a dynamic revision, reset to the 
                    // asked revision
                    depResolvedId = depRevisionId
                    depDescriptor = null
                  }

                  if (depResolvedId == null)
                    throw new NullPointerException("getResolvedId() is null for " + dep.toString())

                  if (depRevisionId == null)
                    throw new NullPointerException("getDependencyRevisionId() " + "is null for " + dd.toString())

                  val rev = depResolvedId.getRevision()
                  val forcedRev =
                    if (forcedRevisionId == null) rev
                    else forcedRevisionId.getRevision()

                  // The evicted modules have no description, so we can't put the status
                  val status =
                    if (depDescriptor == null) "?"
                    else depDescriptor.getStatus()

                  // Message.debug("storing dependency " + depResolvedId + " in props")
                  // props.put(depRevisionId.encodeToString(), rev + " " + status + " " + forcedRev + " " + depResolvedId.getBranch())
                  Some((depRevisionId.encodeToString(), rev + " " + status + " " + forcedRev + " " + depResolvedId.getBranch()))
                } else None
              } else None
            }
        }

        println("RESOLVE IMPL 10")

        Await.result(Future.sequence(depsProcessing), Duration.Inf).flatten.foreach { p =>
          Message.debug("storing dependency " + p._1 + " in props")
          props.put(p._1, p._2)
        }
      }

      println("RESOLVE IMPL 11")

      val out = new FileOutputStream(ivyPropertiesInCache)
      props.store(out, md.getResolvedModuleRevisionId() + " resolved revisions")
      out.close()

      Message.verbose("\tresolved ivy file produced in cache")

      report.setResolveTime(System.currentTimeMillis() - start)

      println("RESOLVE IMPL 12")
      println("DOWNLOADING")

      if (options.isDownload()) {
        Message.verbose(":: downloading artifacts ::")

        downloadArtifacts(report, options.getArtifactFilter(),
          new DownloadOptions().setLog(options.getLog()).asInstanceOf[DownloadOptions])
      }

      println("RESOLVE IMPL 13")
      if (options.isOutputReport()) {
        outputReport(report, cacheManager, options)
      }

      Message.verbose("\tresolve done (" + report.getResolveTime() + "ms resolve - "
        + report.getDownloadTime() + "ms download)")
      Message.sumupProblems()

      eventManager.fireIvyEvent(new EndResolveEvent(md, confs, report))
      report
    } catch {
      case ex: RuntimeException =>
        Message.error(ex.getMessage())
        Message.sumupProblems()
        throw ex
    } finally {
      context.setResolveData(null)
      setDictatorResolver(oldDictator)
    }
  }

  override def getDependencies(md: ModuleDescriptor, options: ResolveOptions, report: ResolveReport): Array[IvyNode] = {
    println("GET DEPENDENCIES")
    val res = super.getDependencies(md, options, report)
    println("DEPENDENCIES GOT")
    res
  }

  override def downloadArtifacts(
    report: ResolveReport,
    artifactFilter: Filter,
    options: DownloadOptions
  ): Unit = {
    import scala.collection.JavaConverters._
    val start = System.currentTimeMillis
    report.getArtifacts match {
      case typed: java.util.List[Artifact @unchecked] =>
        new PrepareDownloadEvent(typed.asScala.toArray)
    }
    // Farm out the dependencies for parallel download
    implicit val ec = ParallelResolveEngine.resolveExecutionContext
    val allDownloadsFuture = Future.traverse(report.getDependencies.asScala) {
      case dep: IvyNode =>
        Future {
          if (!(dep.isCompletelyEvicted || dep.hasProblem) &&
            dep.getModuleRevision != null) {
            Some(downloadNodeArtifacts(dep, artifactFilter, options))
          } else None
        }
    }
    val allDownloads = Await.result(allDownloadsFuture, Duration.Inf)
    //compute total downloaded size
    val totalSize = allDownloads.foldLeft(0L) {
      case (size, Some(download)) =>
        val dependency = download.dep
        val moduleConfigurations = dependency.getRootModuleConfigurations
        moduleConfigurations.foreach { configuration =>
          val configurationReport = report.getConfigurationReport(configuration)

          // Take into account artifacts required by the given configuration
          if (dependency.isEvicted(configuration) ||
            dependency.isBlacklisted(configuration)) {
            configurationReport.addDependency(dependency)
          } else configurationReport.addDependency(dependency, download.report)
        }

        size + download.totalSizeDownloaded
      case (size, None) => size
    }

    report.setDownloadTime(System.currentTimeMillis() - start)
    report.setDownloadSize(totalSize)
  }

  /**
   * Download all the artifacts associated with an ivy node.
   *
   * Return the report and the total downloaded size.
   */
  private def downloadNodeArtifacts(
    dependency: IvyNode,
    artifactFilter: Filter,
    options: DownloadOptions
  ): DownloadResult = {

    val resolver = dependency.getModuleRevision.getArtifactResolver
    val selectedArtifacts = dependency.getSelectedArtifacts(artifactFilter)
    val downloadReport = resolver.download(selectedArtifacts, options)
    val artifactReports = downloadReport.getArtifactsReports

    val totalSize = artifactReports.foldLeft(0L) { (size, artifactReport) =>
      // Check download status and report resolution failures
      artifactReport.getDownloadStatus match {
        case DownloadStatus.SUCCESSFUL =>
          size + artifactReport.getSize
        case DownloadStatus.FAILED =>
          val artifact = artifactReport.getArtifact
          val mergedAttribute = artifact.getExtraAttribute("ivy:merged")
          if (mergedAttribute != null) {
            Message.warn(s"\tMissing merged artifact: $artifact, required by $mergedAttribute.")
          } else {
            Message.warn(s"\tDetected merged artifact: $artifactReport.")
            resolver.reportFailure(artifactReport.getArtifact)
          }
          size
        case _ => size
      }
    }

    DownloadResult(dependency, downloadReport, totalSize)
  }
}
