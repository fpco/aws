{-# LANGUAGE GeneralizedNewtypeDeriving, DeriveGeneric #-}
module Aws.DynamoDb.Commands.Item(
  -- * Commands
    getItem
  , GetItem(..)
  , GetItemResult(..)
  , PutItem(..)
  , PutItemResult(..)
  , query
  , Query(..)
  , QueryResult(..)

  -- * Data passed in the commands
  , Attributes
  , AttributeValue(..)
  , KeyConditions
  , KeyCondition(..)
  , ComparisonOperator(..)
) where

import           Aws.Core
import           Aws.DynamoDb.Core
import           Control.Applicative
import           Data.Aeson ((.=), (.:))
import qualified Data.Aeson as A
--EKB XXX import qualified Data.Aeson.Types as A
import qualified Data.ByteString as BS
import qualified Data.ByteString.Base64 as B64
--EKB XXX import           Data.Char (toUpper)
--EKB XXX import           Data.Time
--EKB XXX import           Data.Time.Clock.POSIX
import qualified Data.Text             as T
import qualified Data.Text.Encoding    as T
--EKB XXX import qualified Data.Vector           as V
import qualified Data.HashMap.Strict   as M
import qualified Data.HashSet          as S
--EKB XXX import           GHC.Generics (Generic)

--EKB XXX comment about missing operations and options
--EKB XXX retry on ProvisionedThroughputExceededException

-------------------------------------------------------------------------------
--- Commands
-------------------------------------------------------------------------------

data GetItem
    = GetItem {
        getConsistentRead :: Bool
      , getKey :: Attributes
      , getTableName :: T.Text
      }
    deriving (Show)
instance A.ToJSON GetItem where
    --EKB XXX leave out optional attrs when default value?
    toJSON gi = A.object [ "ConsistentRead" .= getConsistentRead gi
                         , "Key" .= getKey gi
                         , "TableName" .= getTableName gi
                         ]
-- | ServiceConfiguration: 'DyConfiguration'
instance SignQuery GetItem where
    type ServiceConfiguration GetItem = DyConfiguration
    signQuery = dySignQuery "GetItem"

getItem :: Attributes -> T.Text -> GetItem
getItem key tableName = GetItem False key tableName

data GetItemResult
    = GetItemResult {
        giItem :: Attributes
      }
    deriving (Show)
instance A.FromJSON GetItemResult where
    parseJSON (A.Object o) = GetItemResult <$> o .: "Item"
    parseJSON _ = fail "Get item result must be an object"
instance ResponseConsumer r GetItemResult where
    type ResponseMetadata GetItemResult = DyMetadata
    responseConsumer _ _ = dyResponseConsumer
instance AsMemoryResponse GetItemResult where
    type MemoryResponse GetItemResult = Attributes
    loadToMemory = return . giItem

instance Transaction GetItem GetItemResult

data PutItem
    = PutItem {
        putItem :: Attributes
      , putTableName :: T.Text
      }
instance A.ToJSON PutItem where
    toJSON pi' = A.object [ "Item" .= putItem pi'
                         , "TableName" .= putTableName pi'
                         ]
-- | ServiceConfiguration: 'DyConfiguration'
instance SignQuery PutItem where
    type ServiceConfiguration PutItem = DyConfiguration
    signQuery = dySignQuery "PutItem"

data PutItemResult
    = PutItemResult {
      }
    deriving (Show)
instance A.FromJSON PutItemResult where
    --EKB XXX derive instead?
    parseJSON (A.Object _) = pure PutItemResult
    parseJSON _ = fail "Put item result must be an object"
instance ResponseConsumer r PutItemResult where
    type ResponseMetadata PutItemResult = DyMetadata
    responseConsumer _ _ = dyResponseConsumer
instance AsMemoryResponse PutItemResult where
    --EKB XXX right way?
    type MemoryResponse PutItemResult = ()
    loadToMemory = return . const ()

instance Transaction PutItem PutItemResult

data Query
    = Query {
        queryConsistentRead :: Bool
      , queryExclusiveStartKey :: Maybe Attributes
      , queryKeyConditions :: KeyConditions
      , queryTableName :: T.Text
      }
    deriving (Show)
instance A.ToJSON Query where
    --EKB XXX leave out optional attrs when default value?
    toJSON q = A.object [ "ConsistentRead" .= queryConsistentRead q
                        , "ExclusiveStartKey" .= queryExclusiveStartKey q
                        , "KeyConditions" .= queryKeyConditions q
                        , "TableName" .= queryTableName q
                        ]
-- | ServiceConfiguration: 'DyConfiguration'
instance SignQuery Query where
    type ServiceConfiguration Query = DyConfiguration
    signQuery = dySignQuery "Query"

query :: KeyConditions -> T.Text -> Query
query keyConditions tableName = Query False Nothing keyConditions tableName

data QueryResult
    = QueryResult {
        qCount :: Integer
      , qItems :: [Attributes]
      , qLastEvaluatedKey :: Attributes
      }
    deriving (Show)
instance A.FromJSON QueryResult where
    parseJSON (A.Object o) = QueryResult
        <$> o .: "Count"
        <*> o .: "Items"
        <*> o .: "LastEvaluatedKey"
    parseJSON _ = fail "Query result must be an object"
instance ResponseConsumer r QueryResult where
    type ResponseMetadata QueryResult = DyMetadata
    responseConsumer _ _ = dyResponseConsumer
instance AsMemoryResponse QueryResult where
    type MemoryResponse QueryResult = QueryResult
    loadToMemory = return

instance Transaction Query QueryResult

-------------------------------------------------------------------------------
--- Data passed in the commands
-------------------------------------------------------------------------------

type Attributes = M.HashMap T.Text AttributeValue

data AttributeValue
    = AttrBinary BS.ByteString
    | AttrBinarySet (S.HashSet BS.ByteString)
    | AttrNumber Double
    | AttrNumberSet (S.HashSet Double)
    | AttrString T.Text
    | AttrStringSet (S.HashSet T.Text)
    deriving (Show)
instance A.ToJSON AttributeValue where
    toJSON (AttrBinary b)     = A.object ["B" .= T.decodeUtf8 (B64.encode b)]
    toJSON (AttrBinarySet bs) = A.object ["BS" .= S.map (T.decodeUtf8 . B64.encode) bs]
    toJSON (AttrNumber n)     = A.object ["N" .= n]
    toJSON (AttrNumberSet ns) = A.object ["NS" .= ns]
    toJSON (AttrString s)     = A.object ["S" .= s]
    toJSON (AttrStringSet ss) = A.object ["SS" .= ss]
instance A.FromJSON AttributeValue where
    parseJSON (A.Object o) = AttrBinary <$> (parseBase64 =<< o .: "B")
                         <|> AttrBinarySet . S.fromList <$> (mapM parseBase64 =<< o .: "BS")
                         <|> AttrNumber <$> o .: "N"
                         <|> AttrNumberSet <$> o .: "NS"
                         <|> AttrString <$> o .: "S"
                         <|> AttrString <$> o .: "SS"
      where
        parseBase64 = either fail return . B64.decode . T.encodeUtf8
    parseJSON _ = fail "Attribute value must be an object"

type KeyConditions = M.HashMap T.Text KeyCondition

data KeyCondition
    = KeyCondition {
        keyAttributeValueList :: [AttributeValue]
      , keyComparisonOperator :: ComparisonOperator
      }
    deriving (Show)
instance A.ToJSON KeyCondition where
    toJSON kc = A.object [ "AttributeValueList" .= keyAttributeValueList kc
                         , "ComparisonOperator" .= keyComparisonOperator kc
                         ]

data ComparisonOperator
    = ComparisonEQ
    | ComparisonLE
    | ComparisonLT
    | ComparisonGE
    | ComparisonGT
    | ComparisonBeginsWith
    | ComparisonBetween
    deriving (Show, Eq, Enum, Bounded)
instance A.ToJSON ComparisonOperator where
    toJSON ComparisonEQ = "EQ"
    toJSON ComparisonLE = "LE"
    toJSON ComparisonLT = "LT"
    toJSON ComparisonGE = "GE"
    toJSON ComparisonGT = "GT"
    toJSON ComparisonBeginsWith = "BEGINS_WITH"
    toJSON ComparisonBetween = "BETWEEN"
instance A.FromJSON ComparisonOperator where
    parseJSON (A.String str) =
        case str of
            "EQ" -> return ComparisonEQ
            "LE" -> return ComparisonLE
            "LT" -> return ComparisonLT
            "GE" -> return ComparisonGE
            "GT" -> return ComparisonGT
            "BEGINS_WITH" -> return ComparisonBeginsWith
            "BETWEEN" -> return ComparisonBetween
            _   -> fail $ "Invalid comparison operator " ++ T.unpack str
    parseJSON _ = fail "Comparison operator must be a string"
