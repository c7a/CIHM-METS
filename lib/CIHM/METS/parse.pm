package CIHM::METS::parse;

use 5.006;
use strict;
use warnings FATAL => 'all';
use XML::LibXML;
use Switch;
use File::Basename;

# using iso8601 and cmr_lang , but will explicitly reference namespace
# as we want to clean up these functions to separate libraries.
use lib "/opt/tdr/current/lib";
use CIHM::CMR;

=head1 NAME

CIHM::METS::parse - Parse METS records that conform to the Canadiana Application profile

=head1 VERSION

Version 0.03


=cut

our $VERSION = '0.03';


=head1 SYNOPSIS

This module parses XML METS records and returns hashes which are stored in and
distributed by CouchDB and indexed by Solr.

    use CIHM::METS::parse;

    my $foo = CIHM::METS::parse->new($args);
    where $args is a HASH containing parameters
       aip - AIP ID (depositor.OBJID)
       metspath - Path within the AIP to the METS file (IE: /data/sip/data/metadata.xml)
       xmlfile - String containing the contents of the METS file
       metsaccess - Object which has a function $metsaccess->get_metadata($file) which is able to return the contents of $file within the AIP.



We know we have more documentation to do, but want to make this source visible
sooner.

=head1 AUTHOR

Sascha Adler, C<< <sascha.adler at canadiana.ca> >>
Russell McOrmond, C<< <russell.mcormond at canadiana.ca> >>
Julienne Pascoe, C<< <julienne.pascoe at canadiana.ca> >>

=head1 BUGS

Please report any bugs or feature requests through the web interface at
L<https://github.com/c7a/CIHM-METS-parse>.  We will be notified, and then you'll
automatically be notified of progress on your bug as we make changes.

=cut



sub new {
    my($class, $args) = @_;
    my $self = bless {}, $class;

    if (ref($args) ne "HASH") {
        die "Argument to CIHM::METS::parse->new() not a hash\n";
    };
    $self->{args} = $args;

    $self->{xml}=XML::LibXML->new->parse_string($self->xmlfile);
    $self->{xpc}=XML::LibXML::XPathContext->new,
    $self->{fileinfo}={};

    my ($depositor,$objid)=split(/\./,$self->aip);
    $self->{depositor}=$depositor;
    $self->{objid}=$objid;

    $self->xpc->registerNs('mets', "http://www.loc.gov/METS/");
    $self->xpc->registerNs('xlink', "http://www.w3.org/1999/xlink");

    return $self;
}
sub args {
    my $self = shift;
    return $self->{args};
}
sub xmlfile {
    my $self = shift;
    return $self->args->{xmlfile};
}
sub metspath {
    my $self = shift;
    return $self->args->{metspath};
}
sub aip {
    my $self = shift;
    return $self->args->{aip};
}
sub metsaccess {
    my $self = shift;
    return $self->args->{metsaccess};
}
sub xml {
    my $self = shift;
    return $self->{xml};
}
sub xpc {
    my $self = shift;
    return $self->{xpc};
}
sub fileinfo {
    my ($self,$type) = @_;
    return $self->{fileinfo}->{$type};
}
sub depositor {
    my $self = shift;
    return $self->{depositor};
}
sub objid {
    my $self = shift;
    return $self->{objid};
}

sub aipfile {
    my ($self,$type,$loctype,$href) = @_;

    my ($junk1,$metsdir,$junk2)=File::Spec->splitpath($self->metspath);

    if ($loctype eq 'URN') {
        if ($type eq 'FLocat') {
            $href="files/$href";
        } else {
            $href="metadata/$href";
        }
    }
    return substr(File::Spec->rel2abs($href,'//'.$metsdir),1);
}

sub mets_walk_structMap {
       my ($self,$type) = @_;

       my %fi;
       my @divs;

       my @nodes = $self->xpc->findnodes("descendant::mets:structMap[\@TYPE=\"$type\"]",$self->xml);
       if (scalar(@nodes) != 1) {
           die "Found ".scalar(@nodes)." structMap(TYPE=$type)\n";
       }
       foreach my $div ($self->xpc->findnodes('descendant::mets:div',$nodes[0])) {
           my %attr;
           $attr{'type'}=$div->getAttribute('TYPE');
           $attr{'label'}=$div->getAttribute('LABEL');
           my $dmdid=$div->getAttribute('DMDID');
           if ($dmdid) {
               my @dmdsec=$self->xpc->findnodes("descendant::mets:dmdSec[\@ID=\"$dmdid\"]",$self->xml);
               if (scalar(@dmdsec) != 1) {
                   die "Found ".scalar(@dmdsec)." dmdSec for ID=$dmdid\n";
               }
               my @md=$dmdsec[0]->nonBlankChildNodes();
               if (scalar(@md) != 1) {
                   die "Found ".scalar(@md)." children for dmdSec ID=$dmdid\n";
               }
               my @types=split(/:/,$md[0]->nodeName);
               my $type=pop(@types);

               $attr{'dmd.id'}=$dmdid;
               $attr{'dmd.type'}=$type;
               $attr{'dmd.mime'}=$md[0]->getAttribute('MIMETYPE');
               $attr{'dmd.mdtype'}=$md[0]->getAttribute('MDTYPE');
               if ($attr{'dmd.mdtype'} eq 'OTHER') {
                   $attr{'dmd.mdtype'}=$md[0]->getAttribute('OTHERMDTYPE');
               }
               # TODO: Handle $type=mdRef , not needed at this point.
           }
           
           foreach my $fptr ($self->xpc->findnodes('mets:fptr',$div)) {
               my $fileid=$fptr->getAttribute('FILEID');

               my @file=$self->xpc->findnodes("descendant::mets:file[\@ID=\"$fileid\"]",$self->xml);
               if (scalar(@file) != 1) {
                   die "Found ".scalar(@file)." for file ID=$fileid\n";
               }
               my $use=$file[0]->getAttribute('USE');

               # If the file doesn't have USE=, check parent fileGrp
               if (! $use) {
                   my $filegrp=$file[0]->parentNode;
                   $use=$filegrp->getAttribute('USE');
                   if (! $use) {
                       die "Can't find USE= attribute for file ID=$fileid\n";
                   }
               }

               # never used...
               next if $use eq 'canonical';

               my $mimetype = $file[0]->getAttribute('MIMETYPE');

               if ($use eq 'derivative') {
                   if ($mimetype eq 'application/xml') {
                       $use = 'ocr';
                   } elsif ($mimetype eq 'application/pdf') {
                       $use = 'distribution';
                   }
               }

               my @flocat=$self->xpc->findnodes("mets:FLocat",$file[0]);
               if (scalar(@flocat) != 1) {
                   die "Found ".scalar(@flocat)." FLocat file ID=$fileid\n";
               }

               $attr{$use.'.mimetype'}=$mimetype;
               $attr{$use.'.flocat'}=$self->aipfile('FLocat',$flocat[0]->getAttribute('LOCTYPE'),$flocat[0]->getAttribute('xlink:href'));


               my $admid=$file[0]->getAttribute('ADMID');
               # If there is JHOVE, add that information as well
               if ($admid) {
                   my @techmd=$self->xpc->findnodes("descendant::mets:techMD[\@ID=\"$admid\"]",$self->xml);
                   if (scalar(@techmd) != 1) {
                       die "Found ".scalar(@techmd)." for file ID=$fileid\n";
                   }
                   my @mdref=$self->xpc->findnodes("mets:mdRef",$techmd[0]);
                   if (scalar(@mdref) != 1) {
                       die "Found ".scalar(@mdref)." mdRef for file ID=$fileid\n";
                   }
                   if ($mdref[0]->getAttribute('OTHERMDTYPE') eq 'jhove') {
                       $attr{$use.'.jhove'}=$self->aipfile('mdRef',$mdref[0]->getAttribute('LOCTYPE'),$mdref[0]->getAttribute('xlink:href'));
                   } else {
                       die "Found non-jhove metadata for file ID=$fileid\n";
                   }
               }



           }

           push @divs, \%attr;
           if (exists $attr{'master.flocat'}) {
               $fi{$attr{'master.flocat'}}={
                   'use' => 'master',
                   'index' => scalar(@divs)-1
               };
           }
           if (exists $attr{'distribution.flocat'}) {
               $fi{$attr{'distribution.flocat'}}={
                   'use' => 'distribution',
                   'index' => scalar(@divs)-1
               };
           }
       }

       # Store for multiple use
       $self->{fileinfo}->{$type}={
           fileindex => \%fi,
           divs => \@divs
       }
}


# Returns array of hashes, where first element is the item followed by each component.
# Field names documented in:
# https://docs.google.com/a/c7a.ca/spreadsheets/d/13FzeZdXElmq0tKQpQAGLnsGGX36E1uP-7K2FDVjVQ3s/edit?usp=sharing
sub metsdata {
    my ($self,$type) = @_;

    my $fileinfo=$self->{fileinfo}->{$type};
    return if (!$fileinfo);

    my @metsdata;

    for my $i  (0 .. scalar(@{$fileinfo->{'divs'}})-1) {
        my $div=$fileinfo->{'divs'}->[$i];
        my $data = {
            depositor => $self->depositor
        };

        my $type;
        switch ($div->{'type'}) {
            case 'serial' { $type = 'series';}
            case 'collection' { $type = 'series';}
            case 'monograph' { $type = 'document';}
            case 'issue' { $type = 'document';}
            else { $type = $div->{'type'};}
        }
        $data->{'type'}=$type;

        if ($i == 0) {
            # This is the item
            $data->{'key'}=$self->aip;
            $data->{'identifier'}=[$self->objid];
        } else {
            # These are components
            $data->{'key'}=$self->aip.".".$i;
            # Is this component level identifier useful?
            $data->{'identifier'}=[$self->objid.".".$i];
            $data->{'seq'}=$i;
            $data->{'pkey'}=$self->aip;
        }
        # The key is always one of the identifiers

        if (exists $div->{'label'}) {
            $data->{'label'} = $div->{'label'};
        }

        if (exists $div->{'master.flocat'}) {
            $data->{'canonicalMaster'} = $self->aip."/".$div->{'master.flocat'};
        }
        if (exists $div->{'master.mimetype'}) {
            $data->{'canonicalMasterMime'} = $div->{'master.mimetype'};
        }
        if (exists $div->{'distribution.flocat'}) {
            $data->{'canonicalDownload'} = $self->aip."/".$div->{'distribution.flocat'};
        }
        if (exists $div->{'distribution.mimetype'}) {
            $data->{'canonicalDownloadMime'} = $div->{'distribution.mimetype'};
        }

        push @metsdata,$data;
    }
    return \@metsdata;
}


# For now we only extract a text string from OCR data.
sub getOCRtxt {
    my ($self,$type,$index) = @_;

    my $fileinfo=$self->{fileinfo}->{$type};
    return if (!$fileinfo);

    my $div=$fileinfo->{'divs'}->[$index];
    return if (!$div);

    my $ocr;
    # Embedded txtmap
    if (exists $div->{'dmd.mdtype'} && $div->{'dmd.mdtype'} eq 'txtmap') {
        my $dmdid=$div->{'dmd.id'};
        my @dmdsec=$self->xpc->findnodes("descendant::mets:dmdSec[\@ID=\"$dmdid\"]",$self->xml);
        if (scalar(@dmdsec) != 1) {
            die "Found ".scalar(@dmdsec)." dmdSec for ID=$dmdid\n";    
        }
        $ocr=$dmdsec[0]->textContent;
    } elsif (exists $div->{'ocr.flocat'} && $div->{'ocr.mimetype'} eq  'application/xml') {
        my $ocrxml = $self->metsaccess->get_metadata($div->{'ocr.flocat'});
        return if (!$ocrxml);
        my $xml= XML::LibXML->new->parse_string($ocrxml);
        my $xpc = XML::LibXML::XPathContext->new($xml);
        $xpc->registerNs('txt', 'http://canadiana.ca/schema/2012/xsd/txtmap');
        $xpc->registerNs('alto', 'http://www.loc.gov/standards/alto/ns-v3');
        if ($xpc->exists('//txt:txtmap',$xml) || $xpc->exists('//txtmap',$xml)) {
            $ocr=$xml->textContent;
        } elsif ($xpc->exists('//alto',$xml) || $xpc->exists('//alto:alto'),$xml) {
            $ocr='';
            foreach my $content ($xpc->findnodes('//*[@CONTENT]',$xml)) {
                $ocr .= " ".$content->getAttribute('CONTENT');
            }
        } else {
            die "Unknown XML schema for ".$div->{'ocr.flocat'}."\n";
        }
    } else {
        # No OCR data
        return;
    }

    # Collapse all whitespace and trim (some formatting newlines/tab in XML)
    $ocr =~ s/\s+/ /g;
    $ocr =~s/^\s+|\s+$//g;
    return $ocr;
}


# TODO: Rewrite to only use XML::LibXML 
# This is based on CIHM::TDR->build_cmr() used to create cmr.xml
# and is followed by the xsl used to convert CMR files to our Solr schema.
sub extract_idata {
    my($self) = @_;

    #
    # Following loosly based on related CIHM::CMR functionality
    #

    # Where the XSL files are
    my $resource = join("/", dirname($INC{"CIHM/METS/parse.pm"}), "resource");

    my $stylesheet = XML::LibXSLT->new->parse_stylesheet(XML::LibXML->new->parse_file(join("/", $resource, "xsl", "tdr.xsl")));

    # Parameters to pass to the stylesheet
    my %params = (
        contributor => $self->depositor,
        filepath => "/".$self->aipfile('FLocat','URN','')."/",
        );

    my $doc = $stylesheet->transform($self->xml, XML::LibXSLT::xpath_to_string(%params));

    # Post-process

    # If the record has no namespace declaration, add one and re-parse
    # the document.  TODO: Needed any more?
    my $xc = XML::LibXML::XPathContext->new($doc);
    $xc->registerNs("cmr", "http://canadiana.ca/schema/2012/xsd/cmr");
    if (! $xc->findnodes('/cmr:recordset')) {
        warn "Upgrading CMR record to namespace-enabled";
        $doc->documentElement->setNamespace("http://canadiana.ca/schema/2012/xsd/cmr", "", 1);
        $doc = XML::LibXML->new->parse_string($doc->toString);
    }

    # Post-process $doc
    $xc = XML::LibXML::XPathContext->new($doc);
    $xc->registerNs("cmr", "http://canadiana.ca/schema/2012/xsd/cmr");

    foreach my $record ($xc->findnodes('/cmr:recordset/cmr:record')) {
        # Replace <lang> elements with normalised values
        my @lang = $xc->findnodes('cmr:lang', $record);
        if (@lang) {
            my $parent = $lang[0]->parentNode();
            my @values = ();
            foreach my $node (@lang) {
                push(@values, $node->findvalue('.'));
            }
            @values = CIHM::CMR::cmr_lang(@values);
            foreach my $value (@values) {
                my $node = $doc->createElement("cmr:lang");
                $node->setNamespace("http://canadiana.ca/schema/2012/xsd/cmr", "cmr", 1);
                $node->appendChild($doc->createTextNode($value));
                $parent->insertBefore($node, $lang[0]);
            }
            foreach my $node (@lang) {
                $parent->removeChild($node);
            }
        }
    }

    # Rewrite identifiers (key, pkey, gkey) so that they contain only legal characters.
    foreach my $key (
        ($xc->findnodes('/cmr:recordset/cmr:record/cmr:key')),
        ($xc->findnodes('/cmr:recordset/cmr:record/cmr:pkey')),
        ($xc->findnodes('/cmr:recordset/cmr:record/cmr:gkey')),
    ) {
        my $value = $key->findvalue('.');
        $value =~ s/[^A-Za-z0-9_\.\-]/_/g;
        $key->removeChildNodes();
        $key->appendChild($doc->createTextNode($value));
    }

    # Convert pubdate values to standardized ISO-8601 dates. If this cannot be
    # done, remove the pubdate field altogether.
    foreach my $pubdate ($xc->findnodes('/cmr:recordset/cmr:record/cmr:pubdate')) {
        my $min = $pubdate->getAttribute('min');
        my $max = $pubdate->getAttribute('max');
        $min = CIHM::CMR::iso8601($min, 0) unless ($min =~ /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
        $max = CIHM::CMR::iso8601($max, 1) unless ($max =~ /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
        if ($min && $max && $min !~ /^0000/ && $max !~ /^0000/) {
            $pubdate->setAttribute('min', $min);
            $pubdate->setAttribute('max', $max);
        }
        else {
            $pubdate->parentNode->removeChild($pubdate);
        }
    }

    # Delete any description fields with zero-length content or illegal attribute values
    foreach my $field ($xc->findnodes('/cmr:recordset/cmr:record/cmr:description/*')) {
        my $name = $field->nodeName();
        my $text = $field->findvalue('.');
        if ($text =~ /^\s*$/) {
            $field->parentNode->removeChild($field);
        }
        elsif ($field->hasAttribute('lang') && $field->getAttribute('lang') !~ /^[a-z][a-z][a-z]$/) {
            my $lang = $field->getAttribute('lang');
            $field->removeAttribute('lang');
        }
    }

    foreach my $field($xc->findnodes('/cmr:recordset/cmr:record/cmr:description/cmr:subject')) {
      my $text = $field->findvalue('.');
      if ($text =~ /\|\|/) {
        my @subjects = split(/\|\|/,$text);
        my $descnode = $field->parentNode;
        $descnode->removeChild($field);
        foreach my $subject (@subjects) {
          #warn("subject: ".$subject);
          if($subject) {
            $descnode->appendTextChild("subject",$subject);
          }

        }
      }
    }
    foreach my $field($xc->findnodes('/cmr:recordset/cmr:record/cmr:description/cmr:note')) {
      my $text = $field->findvalue('.');
      if ($text =~ /\|\|/) {
        my $descnode=$field->parentNode;
        $descnode->removeChild($field);
        #warn("Splitting fields");
        my $new_notes = join(';', split( /\|\|/,$text));
        #warn("notes: $text new_notes: ".$new_notes);
        if($new_notes) {
          $descnode->appendTextChild("note", $new_notes); 
        }
      }
    }

    # Remove some crud commonly found at the end of MARC title and/or 245 fields.
    foreach my $field ($xc->findnodes('/cmr:recordset/cmr:record/cmr:label')) {
        my $text = $field->findvalue('.');
        $text =~ s/[\s\/\-]+$//g;
        $field->removeChildNodes();
        $field->appendChild($doc->createTextNode($text));
    }
    foreach my $field ($xc->findnodes('/cmr:recordset/cmr:record/cmr:description/cmr:title')) {
        my $text = $field->findvalue('.');
        $text =~ s/[\s\/\-]+$//g;
        $field->removeChildNodes();
        $field->appendChild($doc->createTextNode($text));
    }

# This is based on logic within CIHM::Solr
# /lib/CIHM/Meta/Hammer/resource/xsl/hammer2co.xsl replaced lib/CIHM/resources/xsl/cmr2solr.xsl
# This logic now replaces hammer2co.xsl


    my $xslt = XML::LibXSLT->new();
    $stylesheet = $xslt->parse_stylesheet_file("$resource/xsl/hammer2co.xsl");
    my $result = $stylesheet->transform($doc);


    # Extracting XML into simple array which is specific to the old XSLT.
    # A new XSLT can be created which itself outputs JSON, so this becomes
    # redundant.
    my @metaarray;

    my %multivalue = (
        "lang" => 1,
        "identifier" => 1,
        "ti" => 1,
        "au" => 1,
        "pu" => 1,
        "su" => 1,
        "no" => 1,
        "ab" => 1,
        "tx" => 1,
        "no_rights" => 1,
        "no_source" => 1,
        "tag" => 1,
        "tagPerson" => 1,
        "tagName" => 1,
        "tagPlace" => 1,
        "tagDate" => 1,
        "tagNotebook" => 1,
        "tagDescription" => 1
        );

    foreach my $doc ($result->findnodes('/add/doc')) {
        my $dochash = {};
        my $child = $doc->firstChild;
        while ($child) {
            my $field=$child->getAttribute('name');

            # Handle both single and multi-value fields
            if($multivalue{$field}) {
                if(! defined $dochash->{$field}) {
                    $dochash->{$field}=[];
                }
                push $dochash->{$field},$child->textContent;
            } else {
                die "Single value field `$field` already set\n"
                    if (defined $dochash->{$field});
                $dochash->{$field}=$child->textContent;
            }
            $child=$child->nextSibling();
        }
        push @metaarray, $dochash;
    }
    return \@metaarray;
}

1;
