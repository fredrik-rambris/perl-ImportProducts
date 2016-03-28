#!/usr/bin/env perl
use strict;
use warnings;
use 5.010;

use XML::Simple;
#use Data::Dumper;
use DBI;
use Config::Simple;


=pod

=head1 DESCRIPTION

Imports products from an XML file into a MySQL database

=cut

# Read config
my %config;
Config::Simple->import_from('app.ini', \%config)
    or die Config::Simple->error();


# Set up stuff
my $filename = $ARGV[0];
my $xml = new XML::Simple;

# Connect to database
my $dbh = DBI->connect($config{'database.dsn'}, $config{'database.username'}, $config{'database.password'},
		 {RaiseError => 0, PrintError => 0, mysql_enable_utf8 => 1, AutoCommit=>0}
	) or die("Connect to database failed");

# Prepare our statements

# This one isn't too nice as it increments the primary key on every query and probably should be solved a bit better
# but this was a quick and easy hack to get it done in a one query.
my $binding_statement = $dbh->prepare('INSERT INTO `bindings` SET `binding`=? ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)')
	or die "Couldn't prepare statement: " . $dbh->errstr;

my $product_statement = $dbh->prepare(<<'END_SQL'
	INSERT INTO `products`
		SET 
			`isbn`=?,
			`title`=?,
			`publisher`=?,
			`published_on`=?,
			`language`=?,
			`price`=?,
			`vat`=?,
			`mediatype`=?,
			`binding_id`=?,
			`distributor_name`=?,
			`catalog_text`=?,
			`saga`=?,
			`num_pages`=?,
			`width`=?,
			`height`=?,
			`length`=?
		ON DUPLICATE KEY UPDATE 
			`isbn`=VALUES(`isbn`),
			`title`=VALUES(`title`),
			`publisher`=VALUES(`publisher`),
			`published_on`=VALUES(`published_on`),
			`language`=VALUES(`language`),
			`price`=VALUES(`price`),
			`vat`=VALUES(`vat`),
			`mediatype`=VALUES(`mediatype`),
			`binding_id`=VALUES(`binding_id`),
			`distributor_name`=VALUES(`distributor_name`),
			`catalog_text`=VALUES(`catalog_text`),
			`saga`=VALUES(`saga`),
			`num_pages`=VALUES(`num_pages`),
			`width`=VALUES(`width`),
			`height`=VALUES(`height`),
			`length`=VALUES(`length`)
END_SQL
	) or die "Couldn't prepare statement: " . $dbh->errstr;

my $get_author_statement = $dbh->prepare('SELECT `id` FROM `authors` WHERE `author`=?')
	or die "Couldn't prepare statement: " . $dbh->errstr;

my $insert_author_statement = $dbh->prepare('INSERT INTO `authors` (`author`) VALUES (?) ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)')
	or die "Couldn't prepare statement: " . $dbh->errstr;

my $delete_product_author_statement = $dbh->prepare('DELETE FROM `product_authors` WHERE `product_id`=?')
	or die "Couldn't prepare statement: " . $dbh->errstr;

my $product_author_statement = $dbh->prepare('REPLACE INTO `product_authors` (`product_id`, `author_id`) VALUES (?, ?)')
	or die "Couldn't prepare statement: " . $dbh->errstr;



# Parse XML
my $articles = $xml->XMLin($filename) or die();

# Import products
ARTICLE:
foreach my $article (@{$articles->{artikel}})
{
	my $distributor;
	if(ref($article->{distributor}) eq 'ARRAY')
	{
		# Simplify by assuming we're only interested in the primary distributor
		foreach( values ($article->{distributor}) )
		{
			if($_->{distributor_id} eq $article->{huvuddistributor_id})
			{
				$distributor = $_;
				last;
			}
		}
	}
	else
	{
		$distributor = $article->{distributor};
	}

	if( ! defined $distributor ) {
		say "Skipped $article->{artikelnummer} due to missing distributor";
		next
	};

	if( !$distributor->{lagersaldo} or $distributor->{lagersaldo} <= 0)
	{
		say "Skipped $article->{artikelnummer} due to no stock";
		next;
	}


	$binding_statement->execute($article->{bandtyp});
	my $binding_id = $binding_statement->{mysql_insertid};

	# Convert fpris to float
	my $fpris = $distributor->{fpris};
	$fpris =~ s/\,/./g;
	$fpris *= 1.0;

	say "Importing $article->{artikelnummer}";

	my $result = $product_statement->execute(
		$article->{artikelnummer},
		$article->{titel},
		$article->{forlag},
		$article->{utgivningsdatum},
		$article->{sprak},
		$fpris,
		$article->{moms},
		$article->{mediatyp},
		$binding_id,
		$distributor->{distributor_namn},
		$article->{katalogtext},
		$article->{saga},
		$article->{omfang}->{content},
		$article->{ryggbredd},
		$article->{hojd},
		$article->{bredd}
	);
	if(!$result)
	{
		say $product_statement->errstr;
		next;
	}
	my $product_id = $product_statement->{mysql_insertid};

	# Clear existing product authors
	$delete_product_author_statement->execute($product_id);

	if(ref($article->{medarbetare}) eq 'ARRAY')
	{
		foreach( values ($article->{medarbetare}) )
		{
			if($_->{typ} eq "forfattare")
			{
				Add_Author($product_id, $_->{content});
			}
		}
	}
	elsif($article->{medarbetare}->{typ} eq "forfattare")
	{
		Add_Author($product_id, $article->{medarbetare}->{content});
	}
	$dbh->commit();
}

sub Add_Author {
	my ($product_id, $name) = @_;
	my $author_id;

	if($get_author_statement->execute($name))
	{
		my @row=$get_author_statement->fetchrow_array;
		if(@row)
		{
			$author_id=$row[0];
		}
		else
		{
			$insert_author_statement->execute($name);
			$author_id=$insert_author_statement->{mysql_insertid};
		}
	}
	$product_author_statement->execute($product_id, $author_id);
}