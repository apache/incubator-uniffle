package org.apache.uniffle.common.web.resource;

import javax.servlet.ServletContext;

import io.prometheus.client.CollectorRegistry;

import org.apache.uniffle.common.exception.InvalidRequestException;

public abstract class BaseMetricResource {

  protected CollectorRegistry getCollectorRegistry(ServletContext servletContext, String type) {
    CollectorRegistry registry = (CollectorRegistry) servletContext.getAttribute(
        CollectorRegistry.class.getCanonicalName() + "#" + type);
    if (registry == null) {
      throw new InvalidRequestException(String.format("Metric type[%s] not supported", type));
    }
    return registry;
  }
}
