module Aws.DynamoDb.Commands.GetItem
where

import           Aws.Core
import           Aws.DynamoDb.Core
import           Control.Applicative
import           Control.Monad.Trans.Resource (ResourceT)
import           Data.ByteString.Char8 ({- IsString -})
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy  as L
import qualified Data.Conduit          as C
import           Data.Map
import           Data.Maybe
import qualified Data.Text             as T
import qualified Data.Text.Encoding    as T
import qualified Network.HTTP.Conduit  as HTTP
import qualified Network.HTTP.Types    as HTTP

data GetItem
    = GetItem {
        giKey :: Map T.Text AttributeValue
      , giTableName :: T.Text
      , giAttributesToGet :: [T.Text]
      , giConsistentRead :: Maybe Bool
      , giReturnConsumedCapacity :: Maybe T.Text
      , giResponseContentType :: Maybe T.Text
      , giResponseContentLanguage :: Maybe T.Text
      , giResponseExpires :: Maybe T.Text
      , giResponseCacheControl :: Maybe T.Text
      , giResponseContentDisposition :: Maybe T.Text
      , giResponseContentEncoding :: Maybe T.Text
      , giResponseContentRange :: Maybe (Int,Int)
      }
  deriving (Show)

getItem :: Map T.Text AttributeValue -> T.Text -> GetItem
getItem ks tn = GetItem ks tn [] Nothing Nothing Nothing Nothing Nothing Nothing Nothing Nothing Nothing

data GetItemResponse
    = GetItemResponse {
        girResponse :: HTTP.Response (C.ResumableSource (ResourceT IO) B8.ByteString)
      }

data GetItemMemoryResponse
    = GetItemMemoryResponse (HTTP.Response L.ByteString)
    deriving (Show)

-- | ServiceConfiguration: 'DynamoDbConfiguration'
instance SignQuery GetItem where
    type ServiceConfiguration GetItem = DynamoDbConfiguration
    signQuery GetItem {..} = ddbSignQuery DynamoDbQuery {
                                   ddbQMethod = Get
                                 , ddbQSubresources = []
                                 , ddbQQuery = HTTP.toQuery [
                                                ("response-content-type" :: B8.ByteString,) <$> giResponseContentType
                                              , ("response-content-language",) <$> giResponseContentLanguage
                                              , ("response-expires",) <$> giResponseExpires
                                              , ("response-cache-control",) <$> giResponseCacheControl
                                              , ("response-content-disposition",) <$> giResponseContentDisposition
                                              , ("response-content-encoding",) <$> giResponseContentEncoding
                                              ]
                                 , ddbQContentType = Nothing
                                 , ddbQContentMd5 = Nothing
                                 , ddbQAmzHeaders = []
                                 , ddbQOtherHeaders = catMaybes [
                                                       decodeRange <$> giResponseContentRange
                                                     ]
                                 , ddbQRequestBody = Nothing
                                 }
      where decodeRange (pos,len) = ("range",B8.concat $ ["bytes=", B8.pack (show pos), "-", B8.pack (show len)])

instance ResponseConsumer GetItem GetItemResponse where
    type ResponseMetadata GetItemResponse = DynamoDbMetadata
    responseConsumer GetItem{..} metadata resp
        = do rsp <- ddbBinaryResponseConsumer return metadata resp
             return $ GetItemResponse rsp

instance Transaction GetItem GetItemResponse

instance AsMemoryResponse GetItemResponse where
    type MemoryResponse GetItemResponse = GetItemMemoryResponse
    loadToMemory (GetItemResponse x) = GetItemMemoryResponse <$> HTTP.lbsResponse x
