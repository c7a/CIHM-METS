<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet version="1.0"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:cmr="http://canadiana.ca/schema/2012/xsd/cmr"
  xmlns:mets="http://www.loc.gov/METS/"
  xmlns:marc="http://www.loc.gov/MARC21/slim" 
  xmlns:xlink="http://www.w3.org/1999/xlink"
  xmlns:txt="http://canadiana.ca/schema/2012/xsd/txtmap"
  xmlns:issue="http://canadiana.ca/schema/2012/xsd/issueinfo"
  exclude-result-prefixes="mets marc xlink txt issue"
>

  <!--
    Stylesheet to convert from a CSIP METS record to CMR 1.1.
    The stylesheet must be passed a valid contributor parameter.
  -->

  <!--
    For ease of maintenance, templates for each record type are broken
    into separate files. These files all share the same default namespace,
    so care must be taken not to re-use template names or signatures.
  -->
  <xsl:import href="include/tdr_marc.xsl"/>
  <xsl:import href="include/tdr_simpledc.xsl"/>
  <xsl:import href="include/tdr_issue.xsl"/>

  <xsl:param name="contributor"/>
  <xsl:param name="filepath"/>
  <xsl:param name="root"/>

  <xsl:key name="dmd" match="mets:dmdSec" use="@ID"/>
  <xsl:key name="file" match="mets:file" use="@ID"/>

  <xsl:output method="xml" encoding="UTF-8" indent="yes"/>

  <xsl:template match="mets:mets">
    <cmr:recordset version="1.2">
      <xsl:apply-templates select="mets:structMap/mets:div"/>
    </cmr:recordset>
  </xsl:template>

  <xsl:template match="mets:div">
    
    <xsl:choose>
      <xsl:when test="@TYPE = 'document' or @TYPE = 'series'">
        <!--
          Determine what record format is being used and process it.
        -->
        <xsl:variable name="record" select="key('dmd', @DMDID)"/>
        <xsl:choose>
          <xsl:when test="$record/mets:mdWrap/@MDTYPE = 'DC'">
            <xsl:call-template name="tdr_simpledc">
              <xsl:with-param name="type" select="@TYPE"/>
            </xsl:call-template>
          </xsl:when>
          <xsl:when test="$record/mets:mdWrap/@MDTYPE = 'MARC'">
            <xsl:call-template name="tdr_marc">
              <xsl:with-param name="type" select="@TYPE"/>
            </xsl:call-template>
          </xsl:when>
        </xsl:choose>
      </xsl:when>
      <xsl:when test="@TYPE = 'issue'">
        <xsl:call-template name="tdr_issue"/>
      </xsl:when>
    </xsl:choose>
    <xsl:apply-templates select="mets:div"/>
  </xsl:template>

  <xsl:template match="mets:fptr">
    <xsl:variable name="file" select="key('file', @FILEID)"/>
    <xsl:choose>
      <xsl:when test="$file/../@USE='master'">
        <cmr:canonicalMaster mime="{$file/@MIMETYPE}"><xsl:value-of select="concat($filepath, $file/mets:FLocat[@LOCTYPE='URN']/@xlink:href)"/></cmr:canonicalMaster>
      </xsl:when>
      <xsl:when test="$file/../@USE='distribution'">
        <cmr:canonicalDownload mime="{$file/@MIMETYPE}"><xsl:value-of select="concat($filepath, $file/mets:FLocat[@LOCTYPE='URN']/@xlink:href)"/></cmr:canonicalDownload>
      </xsl:when> 
      <xsl:when test="$file/../@USE='canonical'">
        <xsl:choose>
          <xsl:when test="$file/mets:FLocat[@LOCTYPE='URL']/@xlink:href">
            <cmr:canonicalUri><xsl:apply-templates select="$file/mets:FLocat[@LOCTYPE='URL']/@xlink:href"/></cmr:canonicalUri>
          </xsl:when>
          <xsl:otherwise>
            <cmr:canonicalUri><xsl:value-of select="concat('http://tdr.canadiana.ca/', $contributor, '//mets:mets/@OBJID')"/></cmr:canonicalUri>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:when>
    </xsl:choose>
  </xsl:template>

</xsl:stylesheet>

